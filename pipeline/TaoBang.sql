IF OBJECT_ID(N'dbo.Stg_Jobs_ta', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[Stg_Jobs_ta] (
        [CongViec] NVARCHAR(MAX) NULL,
        [ViTri] NVARCHAR(MAX) NULL,
        [YeuCauKinhNghiem] NVARCHAR(MAX) NULL,
        [MucLuong] NVARCHAR(MAX) NULL,
        [CapBac] NVARCHAR(MAX) NULL,
        [HinhThucLamViec] NVARCHAR(MAX) NULL,
        [CongTy] NVARCHAR(MAX) NULL,
        [QuyMoCongTy] NVARCHAR(MAX) NULL,
        [SoLuongTuyen] NVARCHAR(MAX) NULL,
        [HocVan] NVARCHAR(MAX) NULL,
        [YeuCauUngVien] NVARCHAR(MAX) NULL,
        [MoTaCongViec] NVARCHAR(MAX) NULL,
        [QuyenLoi] NVARCHAR(MAX) NULL,
        [HanNopHoSo] NVARCHAR(MAX) NULL,
        [LinkBaiTuyenDung] NVARCHAR(MAX) NULL,
        [Nguon] NVARCHAR(MAX) NULL,
        [NgayCaoDuLieu] DATE NULL
    );
    PRINT 'Da tao bang [dbo].[Stg_Jobs_ta].';
END
ELSE
BEGIN
    PRINT 'Bang [dbo].[Stg_Jobs_ta] da ton tai.';
END