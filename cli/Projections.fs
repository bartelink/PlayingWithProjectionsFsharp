module Projections

let run (xs : Event seq) =
    // While there are plenty ways to chain the sequence through each projection in a single pass, this lets one express
    // the derivations as pipeline expressions
    let xs = Seq.cache xs

    let count = Seq.length xs
    printfn "Count: %d" count

    xs
    |> Seq.choose (function
        | { ``type`` = "PlayerHasRegistered" ; payload = p } -> Some p.["player_id"]
        | _ -> None)
    |> Seq.distinct
    |> Seq.length
    |> printfn "Unique players: %d"

    xs
    |> Seq.choose (function
        | { ``type`` = "PlayerHasRegistered" ; timestamp = t } -> Some (t.Year,t.Month)
        | _ -> None)
    |> Seq.countBy id
    |> printfn "Registrations/month: %A"

    let quizNames =
        xs
        |> Seq.choose (function
            | { ``type`` = "QuizWasCreated" ; payload = p } -> (p.["quiz_id"],p.["quiz_title"]) |> Some
            | _ -> None)
        |> Map.ofSeq

    // P1

    xs
    |> Seq.choose (function
        | { ``type`` = "GameWasOpened" ; payload = p } -> Some (p.["game_id"],p.["quiz_id"])
        | _ -> None)
    |> Seq.groupBy snd
    |> Seq.map (fun (qid,gids) -> qid,Seq.length gids)
    |> Seq.sortByDescending snd
    |> Seq.truncate 10
    |> Seq.map (fun (qid,cnt) -> (qid,quizNames.[qid]),cnt)
    |> List.ofSeq
    |> printfn "Most popular games: %A"

    // P2

    let topTenGames (xs : seq<(*gid*)string * (*qid*) string>) =
        xs
        |> Seq.groupBy snd
        |> Seq.map (fun (qid,gids) -> qid,Seq.length gids)
        |> Seq.sortByDescending snd
        |> Seq.truncate 10
        |> Seq.map (fun (qid,cnt) -> (qid,quizNames.[qid]),cnt)
        |> List.ofSeq

    xs
    |> Seq.choose (function
        | { ``type`` = "GameWasOpened" ; payload = p } -> Some (p.["game_id"],p.["quiz_id"])
        | _ -> None)
    |> topTenGames
    |> printfn "Most popular games: %A"

    xs
    |> Seq.choose (function
        | { ``type`` = "GameWasOpened" ; timestamp = t; payload = p } -> Some ((t.Year,t.Month), (p.["game_id"],p.["quiz_id"]))
        | _ -> None)
    |> Seq.groupBy fst
    |> Seq.map (fun (ym,xs) -> ym, (xs |> Seq.map snd |> topTenGames))
    |> printfn "Most popular games per month: %A"

    // P3

    let choicePartition xs =
        let ls,rs = ResizeArray(),ResizeArray()
        xs |> Seq.iter (function Choice1Of2 l -> ls.Add l | Choice2Of2 r -> rs.Add r)
        ls.ToArray(),rs.ToArray()

    let (games, joined) =
        // Run a single pass but without direct mutation. we could
        xs
        |> Seq.choose (function
            | { ``type`` = "GameWasOpened"; payload = p } ->
                Some (Choice1Of2 (p.["game_id"],p.["quiz_id"]))
            | { ``type`` = "PlayerJoinedGame"; timestamp = t; payload = p } ->
                Some (Choice2Of2 ((t.Year,t.Month), p.["game_id"]))
            | _ -> None)
        |> choicePartition
    let gameIdToQuizId = games |> Map.ofSeq
    joined
    |> Seq.groupBy fst
    |> Seq.map (fun (ym,gids) -> ym, (gids |> Seq.map (fun (_ym,gid) -> gid, gameIdToQuizId.[gid]) |> topTenGames))
    |> printfn "Most popular games by per month by player count: %A"

