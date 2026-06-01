(** Body-free HTTP request metadata.

    Runtime adapters receive upload bodies separately through
    {!Runtime.S.with_response}. *)

module Method : sig
  type t = [ `GET | `PUT | `POST | `DELETE | `HEAD | `PATCH ]

  val to_string : t -> string
  val of_string : string -> (t, Error.t) result
  val of_string_exn : string -> t
end

module Target : sig
  type t = private {
    scheme : Endpoint.Scheme.t;
    host : string;
    port : int option;
    path : string;
    query : (string * string list) list;
  }

  val create :
    scheme:Endpoint.Scheme.t ->
    host:string ->
    ?port:int ->
    path:string ->
    ?query:(string * string list) list ->
    unit ->
    (t, Error.t) result

  val create_exn :
    scheme:Endpoint.Scheme.t ->
    host:string ->
    ?port:int ->
    path:string ->
    ?query:(string * string list) list ->
    unit ->
    t

  val authority : t -> string
  val path_and_query : t -> string
end

type t = private {
  method_ : Method.t;
  target : Target.t;
  headers : (string * string) list;
}

val validate_headers : (string * string) list -> (unit, Error.t) result

val create :
  method_:Method.t ->
  target:Target.t ->
  ?headers:(string * string) list ->
  unit ->
  (t, Error.t) result

val create_exn :
  method_:Method.t ->
  target:Target.t ->
  ?headers:(string * string) list ->
  unit ->
  t

val with_headers : t -> (string * string) list -> (t, Error.t) result
val add_header : t -> name:string -> value:string -> (t, Error.t) result
