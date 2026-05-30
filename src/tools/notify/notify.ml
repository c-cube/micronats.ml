(** Desktop notification tool for NATS.

    Subscribes to [user.notify.>] and shows desktop notifications via
    [notify-send]. The subject suffix becomes the notification title; the
    message payload becomes the notification body. *)

let spf = Printf.sprintf

let notify_send ~title ~body =
  let cmd =
    spf "notify-send %s %s" (Filename.quote title) (Filename.quote body)
  in
  ignore (Sys.command cmd)

let main ~host ~port =
  Eio_main.run (fun env ->
      let net = Eio.Stdenv.net env in
      Eio.Switch.run (fun sw ->
          let conn = Mininats.connect_to ~sw ~net ~host ~port () in
          let _sub =
            Mininats.sub conn ~sw ~subject:[ "user"; "notify"; ">" ] (fun msg ->
                let title =
                  match msg.Mininats.subject with
                  | "user" :: "notify" :: rest -> String.concat "." rest
                  | _ -> String.concat "." msg.subject
                in
                notify_send ~title ~body:msg.payload)
          in
          Mininats.wait conn))

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
