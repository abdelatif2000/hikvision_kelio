-- =============================================================
-- Initialize Sync_interface_GED_KELIO database and EmployeeSync table
-- Run this script ONCE on localhost SQL Server before first employee import
-- =============================================================

-- 1. Create the sync database (if it doesn't exist)
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'Sync_interface_GED_KELIO')
BEGIN
    CREATE DATABASE [Sync_interface_GED_KELIO];
    PRINT 'Database Sync_interface_GED_KELIO created.';
END
ELSE
    PRINT 'Database Sync_interface_GED_KELIO already exists.';
GO

USE [Sync_interface_GED_KELIO];
GO

-- 2. Create the EmployeeSync tracking table (if it doesn't exist)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EmployeeSync]') AND type = 'U')
BEGIN
    CREATE TABLE [dbo].[EmployeeSync] (
        [idusers]      INT            NOT NULL PRIMARY KEY,
        [data_hash]    VARBINARY(32)  NOT NULL,
        [last_synced]  DATETIME       NOT NULL DEFAULT GETDATE()
    );
    PRINT 'Table EmployeeSync created.';
END
ELSE
    PRINT 'Table EmployeeSync already exists.';
GO
