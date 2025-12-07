-- 1. Tạo Database TA (Nếu chưa có)
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'TA')
BEGIN
    CREATE DATABASE [TA];
END
GO

USE [TA];
GO

-- 2. Tạo các bảng Dimension
-- Dim_KhuVuc
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_KhuVuc]'))
CREATE TABLE [dbo].[Dim_KhuVuc] (
    [KhuVucID] INT IDENTITY(1,1) PRIMARY KEY,
    [TenKhuVuc] NVARCHAR(50) NOT NULL
);

-- Dim_TinhThanh
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_TinhThanh]'))
CREATE TABLE [dbo].[Dim_TinhThanh] (
    [TinhThanhID] INT IDENTITY(1,1) PRIMARY KEY,
    [TenTinhThanh] NVARCHAR(100) NOT NULL,
    [KhuVucID] INT FOREIGN KEY REFERENCES [dbo].[Dim_KhuVuc]([KhuVucID])
);

-- Dim_CongTy
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_CongTy]'))
CREATE TABLE [dbo].[Dim_CongTy] (
    [CongTyID] INT IDENTITY(1,1) PRIMARY KEY,
    [TenCongTy] NVARCHAR(255) NOT NULL,
);

-- Dim_QuyMoCongTy
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_QuyMoCongTy]'))
CREATE TABLE [dbo].[Dim_QuyMoCongTy] (
    [QuyMoID] INT IDENTITY(1,1) PRIMARY KEY,
    [TenQuyMo] NVARCHAR(50) NOT NULL,

);

-- Dim_HocVan
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_HocVan]'))
CREATE TABLE [dbo].[Dim_HocVan] (
    [HocVanID] INT IDENTITY(1,1) PRIMARY KEY,
    [TrinhDo] NVARCHAR(100) NOT NULL
);

-- Dim_Nguon
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_Nguon]'))
CREATE TABLE [dbo].[Dim_Nguon] (
    [NguonID] INT IDENTITY(1,1) PRIMARY KEY,
    [TenNguon] NVARCHAR(50) NOT NULL
);

-- Dim_CapBac
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_CapBac]'))
CREATE TABLE [dbo].[Dim_CapBac] (
    [CapBacID] INT IDENTITY(1,1) PRIMARY KEY,
    [TenCapBac] NVARCHAR(100) NOT NULL
);

-- Dim_HinhThucLamViec
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_HinhThucLamViec]'))
CREATE TABLE [dbo].[Dim_HinhThucLamViec] (
    [HinhThucID] INT IDENTITY(1,1) PRIMARY KEY,
    [TenHinhThuc] NVARCHAR(100) NOT NULL
);

-- Dim_KieuLamViec
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_KieuLamViec]'))
CREATE TABLE [dbo].[Dim_KieuLamViec] (
    [KieuLamViecID] INT IDENTITY(1,1) PRIMARY KEY,
    [TenKieuLamViec] NVARCHAR(100) NOT NULL
);

-- Dim_Nganh
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_Nganh]'))
CREATE TABLE [dbo].[Dim_Nganh] (
    [NganhID] INT IDENTITY(1,1) PRIMARY KEY,
    [TenNganh] NVARCHAR(255) NOT NULL
);

-- Dim_ThoiGian
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Dim_ThoiGian]'))
CREATE TABLE [dbo].[Dim_ThoiGian] (
    [DateKey] INT PRIMARY KEY,
    [FullDate] DATE NOT NULL,
    [Nam] INT,
    [Quy] INT,
    [Thang] INT,

);

-- 3. Tạo bảng FACT (Tôi đã bổ sung các cột Lương để Dashboard có ý nghĩa)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Fact_JobPostings_DW]'))
CREATE TABLE [dbo].[Fact_JobPostings_DW] (
    [FactID] BIGINT IDENTITY(1,1) PRIMARY KEY,
    [JobHash] VARCHAR(32) NOT NULL,
    
    -- Foreign Keys
    [DateKey] INT,
    [CongTyID] INT DEFAULT -1,
    [QuyMoID] INT DEFAULT -1,
    [TinhThanhID] INT DEFAULT -1,
    [KhuVucID] INT DEFAULT -1,
    [NganhID] INT DEFAULT -1,
    [CapBacID] INT DEFAULT -1,
    [HocVanID] INT DEFAULT -1,
    [HinhThucID] INT DEFAULT -1,
    [KieuLamViecID] INT DEFAULT -1,
    [NguonID] INT DEFAULT -1,
    
    -- Measures
    [SoLuongTuyen] INT DEFAULT 1,

    
    -- Metadata
    [LinkBaiTuyenDung] NVARCHAR(MAX),
    [NgayTao] DATETIME DEFAULT GETDATE(),
    [NgayCapNhat] DATETIME DEFAULT GETDATE(),
    
    INDEX idx_JobHash (JobHash)
);
GO