open OS
open Lwt
open Printf

module IntIntSet = Set.Make(struct
  type t = int * int
  let compare (a1, b1) (a2, b2) =
    let c = compare a1 a2 in
    if c <> 0 then c else compare b1 b2
end)

let _backend = "backend/vbd"

let list_backends h =
  let ls path =
    let abspath = if path = [] then _backend else _backend ^ "/" ^ (String.concat "/" path) in
    try_lwt Xs.directory h abspath with Xs_protocol.Enoent _ -> return [] in
  lwt domids = ls [] in
  let pair x ys = List.map (fun y -> (x, y)) ys in
  lwt backends = Lwt_list.map_s (fun domid ->
    lwt devids = ls [ domid ] in
    return (pair domid devids)
  ) domids in
  let set = List.fold_left (fun set (domid, devid) ->
    try
      IntIntSet.add (int_of_string domid, int_of_string devid) set
    with _ -> set
  ) IntIntSet.empty (List.concat backends) in
  return set

let watch_for_backends () =
  let seen_already = ref IntIntSet.empty in
  Xs.wait (fun h ->
    lwt backends = list_backends h in
    let created = IntIntSet.diff backends !seen_already in
    let destroyed = IntIntSet.diff !seen_already backends in
    IntIntSet.iter (fun (domid, devid) ->
      printf "created: %d.%d\n%!" domid devid;
      let module S = Server.Make(Discard) in
      let configuration = { Storage.filename = ""; format = None } in
      let (_: unit Lwt.t) =
        match_lwt Discard.open_disk configuration with
        | Some t -> S.run t (domid, devid)
        | None -> return () in
      ()
    ) created;
    IntIntSet.iter (fun (domid, devid) ->
      printf "destroyed: %d.%d\n%!" domid devid
    ) destroyed;
    seen_already := backends;
    raise Xs_protocol.Eagain (* keep going forever *)
  )

let main () =
  printf "entering Block_server.main()\n%!";
  printf "starting in 1s\n%!";
  lwt () = OS.Time.sleep 1.0 in
  lwt () = watch_for_backends () in
  printf "shutting down\n%!";
  return ()

