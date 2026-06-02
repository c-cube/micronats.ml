(lang dune 3.0)
(name micronats)
(version 0.1)
(generate_opam_files true)
(authors "Simon Cruanes")
(maintainers "Simon Cruanes")
(homepage "https://github.com/c-cube/micronats.ml")
(bug_reports "https://github.com/c-cube/micronats.ml/issues")
(license "MIT")

(package
 (name micronats)
 (synopsis "Micro NATS client library using Eio")
 (description "A lightweight NATS client using eio")
 (depends
  (ocaml (>= 5.0))
  dune
  logs
  (eio (>= 1.0))
  (eio_posix :with-test))
 (tags (nats messaging pubsub eio)))
