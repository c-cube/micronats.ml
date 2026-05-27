(** Minimal NATS client using Eio.

    Connect to a NATS server, publish, subscribe, and make requests. The
    connection lives on a switch; close it by calling {!close} or finishing the
    switch. *)

type t
(** A connection to a NATS server. *)

type sub
(** A subscription handle. *)

(** {2 Connection} *)

val connect : sw:Eio.Switch.t -> net:_ Eio.Net.t -> unit -> t
(** Connect to [localhost:4222]. *)

val connect_to :
  sw:Eio.Switch.t -> net:_ Eio.Net.t -> host:string -> port:int -> unit -> t
(** Connect to a NATS server at [host]:[port]. *)

(** {2 Messaging} *)

val pub : t -> subject:string -> ?reply_to:string -> string -> unit
(** Publish a message. *)

val sub :
  t ->
  sw:Eio.Switch.t ->
  subject:string ->
  queue:string option ->
  f:(?reply_to:string -> string -> unit) ->
  sub
(** Subscribe to [subject] with an optional [queue] group. The callback receives
    an optional reply-to subject and the payload. Auto-unsubscribed when [sw]
    finishes. *)

val unsub : t -> max_msgs:int option -> sub -> unit
(** Explicitly unsubscribe. *)

val request :
  t ->
  sw:Eio.Switch.t ->
  clock:_ Eio.Time.clock ->
  subject:string ->
  timeout:float ->
  string ->
  (string, [> `Timeout ]) result
(** Send a request, wait for a single reply up to [timeout] seconds. *)

(** {2 Lifecycle} *)

val close : t -> unit
(** Shut down the underlying socket, causing the reader fiber to exit. *)
