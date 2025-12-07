USE [TA];
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_ETL_Load_From_Source_To_DW]
AS
BEGIN
    SET NOCOUNT ON;
    PRINT '>>> [START] ETL: Tuyen_Dung_HN -> TA (FIX DATE ERROR)';

    -- =========================================================================
    -- 1. AUTO-FILL DIMENSIONS
    -- =========================================================================
    PRINT '1. Loading Dims...';

    -- Dim_Nguon
    INSERT INTO [dbo].[Dim_Nguon] ([TenNguon])
    SELECT DISTINCT Nguon FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_Nguon] d WHERE d.TenNguon = src.Nguon) AND src.Nguon IS NOT NULL;

    -- Dim_CapBac
    INSERT INTO [dbo].[Dim_CapBac] ([TenCapBac])
    SELECT DISTINCT CapBac_clean FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_CapBac] d WHERE d.TenCapBac = src.CapBac_clean) AND src.CapBac_clean IS NOT NULL;

    -- Dim_HocVan
    INSERT INTO [dbo].[Dim_HocVan] ([TrinhDo])
    SELECT DISTINCT HocVan_clean FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_HocVan] d WHERE d.TrinhDo = src.HocVan_clean) AND src.HocVan_clean IS NOT NULL;

    -- Dim_Nganh
    INSERT INTO [dbo].[Dim_Nganh] ([TenNganh])
    SELECT DISTINCT LinhVuc_clean FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_Nganh] d WHERE d.TenNganh = src.LinhVuc_clean) AND src.LinhVuc_clean IS NOT NULL;

    -- Dim_HinhThucLamViec
    INSERT INTO [dbo].[Dim_HinhThucLamViec] ([TenHinhThuc])
    SELECT DISTINCT HinhThucLamViec_clean FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_HinhThucLamViec] d WHERE d.TenHinhThuc = src.HinhThucLamViec_clean) AND src.HinhThucLamViec_clean IS NOT NULL;

    -- Dim_KieuLamViec
    INSERT INTO [dbo].[Dim_KieuLamViec] ([TenKieuLamViec])
    SELECT DISTINCT KieuLamViec_clean FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_KieuLamViec] d WHERE d.TenKieuLamViec = src.KieuLamViec_clean) AND src.KieuLamViec_clean IS NOT NULL;

    -- Dim_CongTy
    INSERT INTO [dbo].[Dim_CongTy] ([TenCongTy])
    SELECT DISTINCT CongTy_clean FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_CongTy] d WHERE d.TenCongTy = src.CongTy_clean) AND src.CongTy_clean IS NOT NULL;

    -- Dim_QuyMoCongTy
    INSERT INTO [dbo].[Dim_QuyMoCongTy] ([TenQuyMo])
    SELECT DISTINCT PhanLoaiQuyMoCongTy FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_QuyMoCongTy] d WHERE d.TenQuyMo = src.PhanLoaiQuyMoCongTy) AND src.PhanLoaiQuyMoCongTy IS NOT NULL;

    -- Dim_KhuVuc & TinhThanh
    INSERT INTO [dbo].[Dim_KhuVuc] ([TenKhuVuc])
    SELECT DISTINCT KhuVuc FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_KhuVuc] d WHERE d.TenKhuVuc = src.KhuVuc) AND src.KhuVuc IS NOT NULL;

    INSERT INTO [dbo].[Dim_TinhThanh] ([TenTinhThanh], [KhuVucID])
    SELECT DISTINCT src.Tinh_Thanh, kv.KhuVucID
    FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
    LEFT JOIN [dbo].[Dim_KhuVuc] kv ON src.KhuVuc = kv.TenKhuVuc
    WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_TinhThanh] d WHERE d.TenTinhThanh = src.Tinh_Thanh) AND src.Tinh_Thanh IS NOT NULL;

    -- =========================================================================
    -- 2. CẬP NHẬT DIM THỜI GIAN (Fix lỗi datatype)
    -- =========================================================================
    PRINT '2. Loading Dim_ThoiGian...';
    
    DECLARE @MinDate DATE, @MaxDate DATE;
    -- [FIX]: Dùng TRY_CONVERT để chuyển String sang Date an toàn
    SELECT 
        @MinDate = MIN(TRY_CONVERT(DATE, NgayCaoDuLieu)), 
        @MaxDate = MAX(TRY_CONVERT(DATE, NgayCaoDuLieu)) 
    FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs]
    WHERE TRY_CONVERT(DATE, NgayCaoDuLieu) IS NOT NULL;
    
    IF @MinDate IS NOT NULL
    BEGIN
        WITH DateRange AS (
            SELECT @MinDate AS DateValue
            UNION ALL
            SELECT DATEADD(DAY, 1, DateValue) FROM DateRange WHERE DateValue < @MaxDate
        )
        INSERT INTO [dbo].[Dim_ThoiGian] ([DateKey], [FullDate], [Nam], [Quy], [Thang])
        SELECT 
            CAST(FORMAT(DateValue, 'yyyyMMdd') AS INT),
            DateValue, 
            YEAR(DateValue), 
            DATEPART(QUARTER, DateValue), 
            MONTH(DateValue)
        FROM DateRange src
        WHERE NOT EXISTS (SELECT 1 FROM [dbo].[Dim_ThoiGian] d WHERE d.FullDate = src.DateValue)
        OPTION (MAXRECURSION 0);
    END

    -- =========================================================================
    -- 3. LOAD BẢNG FACT (Fix lỗi datatype)
    -- =========================================================================
    PRINT '3. Loading Fact Table...';

    MERGE INTO [dbo].[Fact_JobPostings_DW] AS Target
    USING (
        SELECT 
            src.JobHash,
            src.LinkBaiTuyenDung,
            
            -- [FIX]: Thêm TRY_CONVERT(DATE, ...) trước khi FORMAT
            CAST(FORMAT(TRY_CONVERT(DATE, src.NgayCaoDuLieu), 'yyyyMMdd') AS INT) AS DateKey,
            
            ISNULL(d_cty.CongTyID, -1) AS CongTyID,
            ISNULL(d_qm.QuyMoID, -1) AS QuyMoID,
            ISNULL(d_tt.TinhThanhID, -1) AS TinhThanhID,
            ISNULL(d_tt.KhuVucID, -1) AS KhuVucID,
            ISNULL(d_nganh.NganhID, -1) AS NganhID,
            ISNULL(d_cb.CapBacID, -1) AS CapBacID,
            ISNULL(d_hv.HocVanID, -1) AS HocVanID,
            ISNULL(d_ht.HinhThucID, -1) AS HinhThucID,
            ISNULL(d_kl.KieuLamViecID, -1) AS KieuLamViecID,
            ISNULL(d_nguon.NguonID, -1) AS NguonID,
            
            ISNULL(src.SoLuongTuyen_clean, 1) AS SoLuongTuyen

        FROM [Tuyen_Dung_HN].[dbo].[fact_clean_jobs] src
        LEFT JOIN [dbo].[Dim_CongTy]        d_cty   ON src.CongTy_clean = d_cty.TenCongTy
        LEFT JOIN [dbo].[Dim_QuyMoCongTy]   d_qm    ON src.PhanLoaiQuyMoCongTy = d_qm.TenQuyMo
        LEFT JOIN [dbo].[Dim_TinhThanh]     d_tt    ON src.Tinh_Thanh = d_tt.TenTinhThanh
        LEFT JOIN [dbo].[Dim_Nganh]         d_nganh ON src.LinhVuc_clean = d_nganh.TenNganh
        LEFT JOIN [dbo].[Dim_CapBac]        d_cb    ON src.CapBac_clean = d_cb.TenCapBac
        LEFT JOIN [dbo].[Dim_HocVan]        d_hv    ON src.HocVan_clean = d_hv.TrinhDo
        LEFT JOIN [dbo].[Dim_HinhThucLamViec] d_ht  ON src.HinhThucLamViec_clean = d_ht.TenHinhThuc
        LEFT JOIN [dbo].[Dim_KieuLamViec]   d_kl    ON src.KieuLamViec_clean = d_kl.TenKieuLamViec
        LEFT JOIN [dbo].[Dim_Nguon]         d_nguon ON src.Nguon = d_nguon.TenNguon

    ) AS Source
    ON (Target.JobHash = Source.JobHash)

    -- UPDATE
    WHEN MATCHED THEN 
        UPDATE SET 
            DateKey = Source.DateKey, CongTyID = Source.CongTyID, QuyMoID = Source.QuyMoID,
            TinhThanhID = Source.TinhThanhID, KhuVucID = Source.KhuVucID, NganhID = Source.NganhID,
            CapBacID = Source.CapBacID, HocVanID = Source.HocVanID, HinhThucID = Source.HinhThucID,
            KieuLamViecID = Source.KieuLamViecID, NguonID = Source.NguonID,
            SoLuongTuyen = Source.SoLuongTuyen,
            NgayCapNhat = GETDATE()

    -- INSERT
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            JobHash, DateKey, CongTyID, QuyMoID, TinhThanhID, KhuVucID, 
            NganhID, CapBacID, HocVanID, HinhThucID, KieuLamViecID, NguonID, 
            SoLuongTuyen, LinkBaiTuyenDung, NgayTao, NgayCapNhat
        )
        VALUES (
            Source.JobHash, Source.DateKey, Source.CongTyID, Source.QuyMoID, Source.TinhThanhID, Source.KhuVucID,
            Source.NganhID, Source.CapBacID, Source.HocVanID, Source.HinhThucID, Source.KieuLamViecID, Source.NguonID,
            Source.SoLuongTuyen, Source.LinkBaiTuyenDung, GETDATE(), GETDATE()
        );

    PRINT '>>> [SUCCESS] HOAN TAT LOAD DATA WAREHOUSE (TA).';
END
GO