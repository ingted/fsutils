#I "C:/Program Files (x86)/Microsoft SQL Server/130/SDK/Assemblies"
#r "Microsoft.SqlServer.Smo.dll"
#r "Microsoft.SqlServer.ConnectionInfo.dll"
#r "Microsoft.SqlServer.SmoExtended.dll"
#r "Microsoft.SqlServer.Management.Sdk.Sfc.dll"

#load "rop.fsx"

open Rop

open Microsoft.SqlServer.Management.Smo

module Internal =
    module FailMessage =
        open System

        let noDatabaseFound server database =
            sprintf "Threre is no database '%s' on server '%s'" database server

        let generic (ex:Exception) =
            sprintf "Failed - %s" (ex.ToString())

        let cantFindServer server (ex:Exception) = 
            sprintf "Can't find server '%s': %s" server (ex.ToString())

        let cantFindDatabase server database (ex:Exception) =
            sprintf "Can't find database '%s' on server '%s' - %s" database server (ex.ToString())

        let cantRestoreDatabase server database (ex:Exception) =
            sprintf "Can't restore database '%s' on server '%s' - %s" database server (ex.ToString())

        let cantCreateDatabase server database (ex:Exception) =
            sprintf "Can't create database '%s' on server '%s' - %s" database server (ex.ToString())

        let cantDropDatabase server database (ex:Exception) =
            sprintf "Can't drop database '%s' on server '%s' - %s" database server (ex.ToString())

    let findServer (serverName:string) =
        Rop.invoke (FailMessage.cantFindServer serverName) <| fun _ -> 
            Server serverName

    let findDatabase databaseName (server:Server) =
        Rop.invokeBind (FailMessage.cantFindDatabase server.Name databaseName) <| fun _ ->
            server.Databases
            |> Seq.cast<Database> 
            |> Seq.tryFind (fun db -> System.String.Compare (db.Name, databaseName, ignoreCase=true) = 0)
            |> Rop.ofOption (FailMessage.noDatabaseFound server.Name databaseName)

    let restore progress bakupPath (server:Server) (database:Database) = 
        Rop.invoke (FailMessage.cantRestoreDatabase server.Name database.Name) <| fun _ ->
            server.KillAllProcesses database.Name
            let restore = Restore ()
            restore.ReplaceDatabase <- true
            restore.Action <- RestoreActionType.Database
            restore.Database <- database.Name
            restore.Devices.AddDevice (bakupPath, DeviceType.File)
            restore.PercentComplete.Add (fun e -> e.Percent |> progress)
            restore.SqlRestore server
            restore.Wait ()

    let restoreDatabase progress (backupPath:string) (serverName:string) (databaseName:string) =
        rop {
            let! server = findServer serverName
            let! database = server |> findDatabase databaseName

            return! restore progress backupPath server database } 

module Database =
    open Internal

    let private printRestorePercent percent = if percent % 10 = 0 then printfn "Restored %d%%" percent
    let private restoreWithProgress = restoreDatabase printRestorePercent

    let restore backupPath serverName databaseName =
        printfn "Starting '%s' database restore on '%s' server from '%s'" databaseName serverName backupPath

        restoreWithProgress backupPath serverName databaseName
        |> function
            | Success _ -> printfn "Successfuly restored database"
            | Failure msg -> printfn "Failed to restore the database: %s" msg

    let create serverName databaseName =
        printfn "Creating database '%s' on '%s' server" databaseName serverName

        Rop.invokeBind (FailMessage.cantCreateDatabase serverName databaseName) <| fun _ -> 
            findServer serverName <!> fun server -> (Database (server,databaseName)).Create ()
        |> function
            | Success _ -> printfn "Successfuly created database"
            | Failure msg -> printfn "Failed to create database: %s" msg

    let drop serverName databaseName =
        printfn "Dropping database '%s' on '%s' server" databaseName serverName

        Rop.invokeBind (FailMessage.cantDropDatabase serverName databaseName) <| fun _ ->
            rop {
                let! server = findServer serverName
                let! database = server |> findDatabase databaseName
                return () |> database.Drop |> ignore }
        |> function
            | Success _ -> printfn "Successfuly dropped database"
            | Failure msg -> printfn "Failed to drop database: %s" msg
