#I "C:/Program Files (x86)/Microsoft SQL Server/130/SDK/Assemblies"
#r "Microsoft.SqlServer.Smo.dll"
#r "Microsoft.SqlServer.ConnectionInfo.dll"
#r "Microsoft.SqlServer.SmoExtended.dll"
#r "Microsoft.SqlServer.Management.Sdk.Sfc.dll"
#r "System.Data"

#load "rop.fsx"

open Rop

open Microsoft.SqlServer.Management.Smo

module Internal =
    open System.Data
    open System.IO

    let private (</>) x y = Path.Combine (x,y)
    let private findColumn failMessage name (columns:DataColumn seq) = 
        columns 
        |> Seq.tryFind (fun c -> System.String.Compare (c.ColumnName,name,ignoreCase=true) = 0)
        |> Rop.ofOption (failMessage name)

    module FailMessage =
        open System

        let noDatabaseFound server database =
            sprintf "Threre is no database '%s' on server '%s'" database server

        let noColumnInBackupFileList backupPath columnName =
            sprintf "There is no column '%s' in filelist table of backup files '%s'" columnName backupPath

        let noDefaultFileGroupFound database = 
            sprintf "There is no default file group in database '%s'" database

        let noFilesInFileGroup fileGroup =
            sprintf "There are no files in file group '%s'" fileGroup

        let noPrimaryDataFile fileGroup = 
            sprintf "There is no primary data file in file group '%s'" fileGroup

        let noDataFileInBackup backup =
            sprintf "There is no data file in backup '%s'" backup

        let noLogFileInBackup backup =
            sprintf "There is no log file in backup '%s'" backup

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
        Rop.invokeBind (FailMessage.cantRestoreDatabase server.Name database.Name) <| fun _ ->
            server.KillAllProcesses database.Name

            let restore = Restore ()
            restore.ReplaceDatabase <- true
            restore.Action <- RestoreActionType.Database
            restore.Database <- database.Name
            restore.Devices.AddDevice (bakupPath, DeviceType.File)

            let files   = server |> restore.ReadFileList
            let columns = files.Columns |> Seq.cast<DataColumn>
            let rows    = files.Rows |> Seq.cast<DataRow>

            let cantFindColumn = FailMessage.noColumnInBackupFileList bakupPath
            let getColumn = findColumn cantFindColumn

            let relocateFiles = 
                rop {
                    let! colLogicalName = columns |> getColumn "LogicalName"
                    let! colType = columns |> getColumn "Type"

                    let! dataFileLogicalName = 
                        rows 
                        |> Seq.tryFind (fun row -> row.[colType].ToString () = "D")
                        |> Rop.ofOption (FailMessage.noDataFileInBackup bakupPath)
                        <!> fun row -> row.[colLogicalName].ToString ()

                    let! logFileLogicalName =
                        rows
                        |> Seq.tryFind (fun row -> row.[colType].ToString () = "L")
                        |> Rop.ofOption (FailMessage.noLogFileInBackup bakupPath)
                        <!> fun row -> row.[colLogicalName].ToString ()

                    let! defaultFileGroup = 
                        database.FileGroups 
                        |> Seq.cast<FileGroup>
                        |> Seq.tryFind (fun fg -> fg.IsDefault) 
                        |> Rop.ofOption (FailMessage.noDefaultFileGroupFound database.Name)

                    let! primaryFile = 
                        if defaultFileGroup.Files.Count = 0 
                        then Failure (FailMessage.noFilesInFileGroup defaultFileGroup.Name)
                        else Success defaultFileGroup.Files
                        >>= (fun files -> 
                            files 
                            |> Seq.cast<DataFile> 
                            |> Seq.tryFind (fun f -> f.IsPrimaryFile)
                            |> Rop.ofOption (FailMessage.noPrimaryDataFile defaultFileGroup.Name))
                        
                    let dataFilePath = server.DefaultFile </> (primaryFile.FileName |> Path.GetFileName)
                    let logFilePath  = server.DefaultLog  </> (sprintf "%s_log.ldf" (dataFilePath |> Path.GetFileNameWithoutExtension))

                    return [ dataFileLogicalName,dataFilePath; logFileLogicalName,logFilePath ] |> List.map RelocateFile }

            relocateFiles
            <!> (fun fs -> 
                fs |> List.map restore.RelocateFiles.Add |> ignore
                restore.PercentComplete.Add (fun e -> e.Percent |> progress)
                restore.SqlRestore server
                restore.Wait ()  |> ignore )

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
