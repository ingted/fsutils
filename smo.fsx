#I "C:/Program Files (x86)/Microsoft SQL Server/130/SDK/Assemblies"
#r "Microsoft.SqlServer.Smo.dll"
#r "Microsoft.SqlServer.ConnectionInfo.dll"
#r "Microsoft.SqlServer.SmoExtended.dll"
#r "Microsoft.SqlServer.Management.Sdk.Sfc.dll"

#load "rop.fsx"

open Rop

open Microsoft.SqlServer.Management.Smo

module Internal =
    let findServer (serverName:string) =
        try
            Success (Server serverName)
        with ex -> 
            Failure (sprintf "Can't find server '%s': %s" serverName (ex.ToString()))

    let findDatabase databaseName (server:Server) =
        rop {
            let database = 
                server.Databases
                |> Seq.cast<Database> 
                |> Seq.tryFind (fun db -> System.String.Compare (db.Name, databaseName, ignoreCase=true) = 0)
                |> Rop.ofOption (sprintf "Can't find database '%s' on server '%s'" databaseName server.Name)

            return! database }

    let restore progress bakupPath (server:Server) (database:Database) = 
        try
            let restore = Restore ()
            restore.ReplaceDatabase <- true
            restore.Action <- RestoreActionType.Database
            restore.Database <- database.Name
            restore.Devices.AddDevice (bakupPath, DeviceType.File)

            server.KillAllProcesses database.Name

            restore.PercentComplete.Add (fun e -> e.Percent |> progress)
            restore.SqlRestore server
            restore.Wait ()

            Success ()
        with ex ->
            Failure (sprintf "Failed to restore the database '%s' on server '%s': %s" database.Name server.Name (ex.ToString()))

    let restoreDatabase progress (backupPath:string) (serverName:string) (databaseName:string) =
        rop {
            let! server = findServer serverName
            let! database = server |> findDatabase databaseName

            return restore progress backupPath server database |> ignore } 
            
let private printRestorePercent percent = if percent % 10 = 0 then printfn "Restored %d%%" percent
let private restoreWithProgress = Internal.restoreDatabase printRestorePercent

let restoreDatabase backupPath serverName databaseName =
    printfn "Starting '%s' database restore on '%s' server from '%s'" databaseName serverName backupPath

    restoreWithProgress backupPath serverName databaseName
    |> function
        | Success _ -> printfn "Successfuly restored database"
        | Failure msg -> printfn "Failed to restor the database: %s" msg

let createDatabase serverName databaseName =
    ()
