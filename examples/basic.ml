open Eio.Std

let ( let@ ) = ( @@ )

let () =
  let@ env = Eio_posix.run in
  let@ sw = Eio.Switch.run ~name:"main" in
  let@ nats = Micronats.with_connect ~sw ~net:(Eio.Stdenv.net env) () in

  let _sub =
    Micronats.sub nats ~sw ~subject:[ "basic"; "test"; ">" ]
      (fun (msg : Micronats.msg) ->
        traceln "received %s" msg.payload;
        ())
  in
  Micronats.wait nats;
  ()
