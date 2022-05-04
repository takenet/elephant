#!/bin/bash
docker run --cap-add SYS_PTRACE -e 'ACCEPT_EULA=1' -e 'MSSQL_SA_PASSWORD=Elephant123!' -p 1433:1433 --name azuresqledge mcr.microsoft.com/azure-sql-edge
