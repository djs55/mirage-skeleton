(*
 * Copyright (C) Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)

open Lwt
open Blkback
open Xs_protocol
open Storage
open OS
open Xs
open Gnt
open Printf

let get_my_domid () =
  immediate (fun xs ->
    try_lwt
      lwt domid = read xs "domid" in
      return (int_of_string domid)
    with Xs_protocol.Enoent _ -> return 0)

let mk_backend_path (domid,devid) =
  lwt self = get_my_domid () in
  return (Printf.sprintf "/local/domain/%d/backend/vbd/%d/%d" self domid devid)

let mk_frontend_path (domid,devid) =
  return (Printf.sprintf "/local/domain/%d/device/vbd/%d" domid devid)

let writev pairs =
  transaction (fun xs ->
    Lwt_list.iter_s (fun (k, v) -> write xs k v) pairs
  )

let readv path keys =
  lwt options = immediate (fun xs ->
    lwt all = directory xs path in
    let has_prefix x prefix =
      let prefix' = String.length prefix and x' = String.length x in
      prefix' <= x' && (String.sub x 0 prefix') = prefix in
    let any f xs = List.fold_left (||) false (List.map f xs) in
    let interesting_keys = List.filter (fun x -> any (has_prefix x) keys) all in
    Lwt_list.map_s (fun k ->
      try_lwt
        lwt v = read xs (path ^ "/" ^ k) in
        return (Some (k, v))
      with _ -> return None) interesting_keys
  ) in
  return (List.fold_left (fun acc x -> match x with None -> acc | Some y -> y :: acc) [] options)

let read_one k = immediate (fun xs ->
  try_lwt
    lwt v = read xs k in
    return (`OK v)
  with _ -> return (`Error ("failed to read: " ^ k)))

let write_one k v = immediate (fun xs -> write xs k v)

let exists k = match_lwt read_one k with `Error _ -> return false | _ -> return true

let max_ring_page_order = ref 0

module Make(S: Storage.S) = struct

let run t (domid,devid) =
  let xg = Gnttab.interface_open () in
  let xe = Eventchn.init () in

  lwt backend_path = mk_backend_path (domid,devid) in

  (* Tell xapi we've noticed the backend *)
  lwt () = write_one
    (backend_path ^ "/" ^ Blkproto.Hotplug._hotplug_status)
    Blkproto.Hotplug._online in

  try_lwt 

    let size = S.size t in
   
    (* Write the disk information for the frontend *)
    let di = Blkproto.({ DiskInfo.sector_size = sector_size;
                         sectors = Int64.div size (Int64.of_int sector_size);
                         media = Media.Disk;
                         mode = Mode.ReadWrite }) in
    let pairs = [
      Blkproto.RingInfo._max_ring_page_order, string_of_int !max_ring_page_order;
    ] @ (Blkproto.DiskInfo.to_assoc_list di) in

    lwt () = writev (List.map (fun (k, v) -> backend_path ^ "/" ^ k, v) pairs) in
    lwt frontend_path = match_lwt read_one (backend_path ^ "/frontend") with
      | `Error x -> failwith x
      | `OK x -> return x in
   
    (* wait for the frontend to enter state Initialised *)
    lwt () = wait (fun xs ->
      try_lwt
        lwt state = read xs (frontend_path ^ "/" ^ Blkproto.State._state) in
        if Blkproto.State.of_string state = Some Blkproto.State.Initialised
        || Blkproto.State.of_string state = Some Blkproto.State.Connected
        then return ()
        else raise Eagain
      with Xs_protocol.Enoent _ -> raise Eagain
    ) in

    lwt frontend = readv frontend_path Blkproto.RingInfo.key_prefixes in
    printf "3 (frontend state=3)\n%!";
    let ring_info = match Blkproto.RingInfo.of_assoc_list frontend with
      | `OK x -> x
      | `Error x -> failwith x in
     
    printf "%s\n%!" (Blkproto.RingInfo.to_string ring_info);
    let device_read page ofs sector_start sector_end =
      try_lwt
        let buf = Cstruct.of_bigarray page in
        let len_sectors = sector_end - sector_start + 1 in
        let len_bytes = len_sectors * sector_size in
        let buf = Cstruct.sub buf (sector_start * sector_size) len_bytes in

        S.read t buf ofs len_sectors
      with e ->
        printf "read exception: %s, offset=%Ld sector_start=%d sector_end=%d\n%!" (Printexc.to_string e) ofs sector_start sector_end;
        Lwt.fail e in
    let device_write page ofs sector_start sector_end =
      try_lwt
        let buf = Cstruct.of_bigarray page in
        let len_sectors = sector_end - sector_start + 1 in
        let len_bytes = len_sectors * sector_size in
        let buf = Cstruct.sub buf (sector_start * sector_size) len_bytes in

        S.write t buf ofs len_sectors
      with e ->
        printf "write exception: %s, offset=%Ld sector_start=%d sector_end=%d\n%!" (Printexc.to_string e) ofs sector_start sector_end;
        Lwt.fail e in
    let be_thread = Blkback.init xg xe domid ring_info Activations.wait {
      Blkback.read = device_read;
      Blkback.write = device_write;
    } in
    lwt () = writev (List.map (fun (k, v) -> backend_path ^ "/" ^ k, v) (Blkproto.State.to_assoc_list Blkproto.State.Connected)) in

    (* wait for the frontend to disappear or enter a Closed state *)
    lwt () = wait (fun xs -> 
      try_lwt
        lwt state = read xs (frontend_path ^ "/state") in
        if Blkproto.State.of_string state <> (Some Blkproto.State.Closed)
        then raise Eagain
        else return ()
      with Xs_protocol.Enoent _ ->
        return ()
    ) in
    Lwt.cancel be_thread;
    Lwt.return ()
  with e ->
    printf "exn: %s\n%!" (Printexc.to_string e);
    return ()
end

