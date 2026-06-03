(** Desktop notification tool for NATS.

    Subscribes to [user.notify.>] and shows desktop notifications via
    [notify-send]. The subject suffix becomes the notification title; the
    message payload becomes the notification body. *)

let notify_send ~sw ~proc_mgr ~title ~body =
  Eio.Fiber.fork ~sw (fun () ->
      match Eio.Process.run proc_mgr [ "notify-send"; title; body ] with
      | () -> ()
      | exception _ -> ())

let main ~host ~port =
  Eio_main.run (fun env ->
      let net = Eio.Stdenv.net env in
      let proc_mgr = Eio.Stdenv.process_mgr env in
      Eio.Switch.run (fun sw ->
          let conn = Micronats.connect ~sw ~net ~host ~port () in
          let _sub =
            Micronats.sub conn ~sw ~subject:[ "user"; "notify"; ">" ]
              (fun msg ->
                let title =
                  match msg.Micronats.subject with
                  | "user" :: "notify" :: rest -> String.concat "." rest
                  | _ -> String.concat "." msg.subject
                in
                notify_send ~sw ~proc_mgr ~title ~body:msg.payload)
          in
          Micronats.wait conn))

let () =
  let host = ref "localhost" in
  let port = ref 4222 in
  let anon _ = () in
  Arg.parse
    [
      "-h", Arg.Set_string host, " NATS host (default: localhost)";
      "--host", Arg.Set_string host, " NATS host (default: localhost)";
      "-p", Arg.Set_int port, " NATS port (default: 4222)";
      "--port", Arg.Set_int port, " NATS port (default: 4222)";
    ]
    anon
    "Usage: notify [OPTIONS]\n\n\
     Listen for user.notify.> messages on NATS and display desktop \
     notifications.";
  main ~host:!host ~port:!port
