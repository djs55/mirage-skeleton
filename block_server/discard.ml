open Lwt
open Storage

(** Used to test the raw ring performance *)

type t = unit

let open_disk _ = return (Some ())
let size () = Int64.(mul (mul 128L 1024L) 1024L)
let read () _ _ _ = return ()
let write () _ _ _ = return ()
