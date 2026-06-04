# micronats: minimal NATS client for OCaml/Eio

[![build](https://github.com/c-cube/micronats.ml/actions/workflows/main.yml/badge.svg)](https://github.com/c-cube/micronats.ml/actions/workflows/main.yml)

A lightweight [NATS](https://nats.io) client library for OCaml >= 5.01 built on [Eio](https://github.com/ocaml-multicore/eio).

## Example

```ocaml
open Eio.Std

let () =
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  Eio.Switch.run @@ fun sw ->
  let@ nats = Micronats.with_connect ~sw ~net () in
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
