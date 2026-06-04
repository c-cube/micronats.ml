# micronats: minimal NATS client for OCaml/Eio

A lightweight [NATS](https://nats.io) client library for OCaml >= 5.01 built on [Eio](https://github.com/ocaml-multicore/eio).

## Features

- Connect with token or user/password auth
- Publish plain messages (`PUB`) and messages with headers (`HPUB`)
- Subscribe with optional queue groups
- Request/reply with inboxes and timeout
- Auto-unsubscribe on switch completion
- Retry helper (`with_retry`)

## Example

```ocaml
open Eio.Std

let () =
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  Eio.Switch.run @@ fun sw ->
  let nats = Micronats.connect ~sw ~net () in
  let@ () = Fun.protect ~finally:(fun () -> Micronats.close nats) in
  let _sub = Micronats.sub nats ~sw ~subject:["greetings"]
    (fun msg -> traceln "received: %s" msg.payload)
  in
  Micronats.pub nats ~subject:["greetings"] "hello, world!"
```

## Install

```sh
opam install micronats
```

## License

MIT
