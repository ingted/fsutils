[<AutoOpen>]
module Rop =
    type Result<'TEntity> =
        | Success of 'TEntity
        | Failure of string

    let map (f: 'a -> 'b) (aResult: Result<'a>) : Result<'b> =
        match aResult with
        | Success a -> Success (f a)
        | Failure m -> Failure m

    let bind (f: 'a -> Result<'b>) (aResult: Result<'a>) : Result<'b> =
        match aResult with
        | Success a -> f a
        | Failure m -> Failure m

    let apply (fResult: Result<('a -> 'b)>) (aResult: Result<'a>) : Result<'b> =
        match fResult, aResult with
        | Success f, Success a -> Success (f a)
        | Failure m, Success _ -> Failure m
        | Success _, Failure m -> Failure m
        | Failure m, Failure m1 -> Failure (m + "; " + m1)

    let iter f = function
        | Success v -> f v |> ignore
        | _ -> ()

    let compose (f: 'a -> Result<'b>) (g: 'b -> Result<'c>) : 'a -> Result<'c> =
        f >> bind g

    let ofOption (failMessage:string) (o: 'a option) =
        match o with
        | Some x -> Success x
        | None -> Failure failMessage

    let private invokeCps onFail onSuccess func =
        try
            () |> func |> onSuccess 
        with ex ->
            ex |> onFail

    let invoke onFail func = 
        func |> invokeCps (onFail >> Failure) Success

    let invokeBind onFail func = 
        func |> invokeCps (onFail >> Failure) id

    let (<!>) o f = map f o
    let (>>=) o f = bind f o
    let (>=>) = compose

    type RopBuilder () =
        member this.Bind(m, f) = bind f m
        member this.Return v = Success v
        member this.ReturnFrom v = v

    let rop = RopBuilder ()
