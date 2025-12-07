CREATE OR ALTER PROCEDURE [dbo].[sp_Import_FactCleanJobs_JSON]
    @JsonData NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. KHAI BÁO BIẾN LOGGING
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @TotalInputRows INT = 0;
    DECLARE @InsertedCount INT = 0;
    DECLARE @UpdatedCount INT = 0;
    DECLARE @ErrorMessage NVARCHAR(MAX) = NULL;
    
    -- Biến bảng để hứng kết quả MERGE
    DECLARE @MergeOutput TABLE (ActionType NVARCHAR(20));

    BEGIN TRY
        ------------------------------------------------------------
        -- 2. ĐỌC JSON & LỌC DỮ LIỆU
        ------------------------------------------------------------
        ;WITH JsonData AS (
            SELECT * FROM OPENJSON(@JsonData)
            WITH (
                JobHash VARCHAR(32) '$.JobHash',
                LinkBaiTuyenDung NVARCHAR(MAX) '$.LinkBaiTuyenDung',
                CongTy NVARCHAR(MAX) '$.CongTy',
                CongTy_clean NVARCHAR(255) '$.CongTy_clean',
                CongViec NVARCHAR(MAX) '$.CongViec',
                CongViec_clean NVARCHAR(255) '$.CongViec_clean',
                CapBac NVARCHAR(100) '$.CapBac',
                CapBac_clean NVARCHAR(100) '$.CapBac_clean',
                ViTri NVARCHAR(MAX) '$.ViTri',
                ViTri_clean NVARCHAR(100) '$.ViTri_clean',
                Tinh_Thanh NVARCHAR(100) '$.Tinh_Thanh',
                KhuVuc NVARCHAR(50) '$.KhuVuc',
                Latitude FLOAT '$.Latitude',
                Longitude FLOAT '$.Longitude',
                MucLuong NVARCHAR(MAX) '$.MucLuong',
                MucLuongMin_clean DECIMAL(18,2) '$.MucLuongMin_clean',
                MucLuongMax_clean DECIMAL(18,2) '$.MucLuongMax_clean',
                MucLuongTB_clean DECIMAL(18,2) '$.MucLuongTB_clean',
                KhoangLuong NVARCHAR(50) '$.KhoangLuong',
                MoTaCongViec NVARCHAR(MAX) '$.MoTaCongViec',
                YeuCauUngVien NVARCHAR(MAX) '$.YeuCauUngVien',
                YeuCauKinhNghiem NVARCHAR(MAX) '$.YeuCauKinhNghiem',
                YeuCauKinhNghiemMin_clean DECIMAL(4,1) '$.YeuCauKinhNghiemMin_clean',
                YeuCauKinhNghiemMax_clean DECIMAL(4,1) '$.YeuCauKinhNghiemMax_clean',
                YeuCauKinhNghiemTB_clean DECIMAL(4,1) '$.YeuCauKinhNghiemTB_clean',
                PhanLoaiKinhNghiem NVARCHAR(100) '$.PhanLoaiKinhNghiem',
                HardSkills NVARCHAR(MAX) '$.HardSkills', 
                SoftSkills NVARCHAR(MAX) '$.SoftSkills',
                LinhVuc NVARCHAR(MAX) '$.LinhVuc',
                LinhVuc_clean NVARCHAR(100) '$.LinhVuc_clean',
                HocVan NVARCHAR(MAX) '$.HocVan',
                HocVan_clean NVARCHAR(100) '$.HocVan_clean',
                HinhThucLamViec NVARCHAR(100) '$.HinhThucLamViec',
                HinhThucLamViec_clean NVARCHAR(100) '$.HinhThucLamViec_clean',
                KieuLamViec_clean NVARCHAR(100) '$.KieuLamViec_clean',
                SoLuongTuyen NVARCHAR(100) '$.SoLuongTuyen',
                SoLuongTuyen_clean INT '$.SoLuongTuyen_clean',
                QuyMoCongTy NVARCHAR(MAX) '$.QuyMoCongTy',
                QuyMoCongTyMin_clean DECIMAL(18,0) '$.QuyMoCongTyMin_clean',
                QuyMoCongTyMax_clean DECIMAL(18,0) '$.QuyMoCongTyMax_clean',
                QuyMoCongTyTB_clean DECIMAL(18,0) '$.QuyMoCongTyTB_clean',
                PhanLoaiQuyMoCongTy NVARCHAR(100) '$.PhanLoaiQuyMoCongTy',
                HanNopHoSo NVARCHAR(50) '$.HanNopHoSo',
                HanNopHoSo_clean NVARCHAR(50) '$.HanNopHoSo_clean',
                Nguon NVARCHAR(50) '$.Nguon',
                NgayCaoDuLieu NVARCHAR(50) '$.NgayCaoDuLieu',
                NgayXuLyDL NVARCHAR(50) '$.NgayXuLyDL'
            )
        ),
        CleanSource AS (
            SELECT 
                *,
                TRY_CONVERT(DATE, HanNopHoSo) AS HanNopHoSo_Date,
                TRY_CONVERT(DATE, HanNopHoSo_clean) AS HanNopHoSo_Clean_Date,
                TRY_CONVERT(DATE, NgayCaoDuLieu) AS NgayCaoDuLieu_Date,
                TRY_CONVERT(DATETIME, NgayXuLyDL) AS NgayXuLyDL_Date
            FROM JsonData
            WHERE CongViec IS NOT NULL AND LTRIM(RTRIM(CongViec)) <> ''
        )

        ------------------------------------------------------------
        -- 3. MERGE (UPSERT)
        ------------------------------------------------------------
        MERGE INTO dbo.fact_clean_job AS Target
        USING CleanSource AS Source
        ON Target.JobHash = Source.JobHash

        WHEN MATCHED THEN
            UPDATE SET
                LinkBaiTuyenDung = Source.LinkBaiTuyenDung,
                CongTy = Source.CongTy, CongTy_clean = Source.CongTy_clean,
                CongViec = Source.CongViec, CongViec_clean = Source.CongViec_clean,
                CapBac = Source.CapBac, CapBac_clean = Source.CapBac_clean,
                ViTri = Source.ViTri, ViTri_clean = Source.ViTri_clean,
                Tinh_Thanh = Source.Tinh_Thanh, KhuVuc = Source.KhuVuc,
                Latitude = Source.Latitude, Longitude = Source.Longitude,
                MucLuong = Source.MucLuong,
                MucLuongMin_clean = Source.MucLuongMin_clean,
                MucLuongMax_clean = Source.MucLuongMax_clean,
                MucLuongTB_clean = Source.MucLuongTB_clean,
                KhoangLuong = Source.KhoangLuong,
                MoTaCongViec = Source.MoTaCongViec,
                YeuCauUngVien = Source.YeuCauUngVien,
                YeuCauKinhNghiem = Source.YeuCauKinhNghiem,
                YeuCauKinhNghiemMin_clean = Source.YeuCauKinhNghiemMin_clean,
                YeuCauKinhNghiemMax_clean = Source.YeuCauKinhNghiemMax_clean,
                YeuCauKinhNghiemTB_clean = Source.YeuCauKinhNghiemTB_clean,
                PhanLoaiKinhNghiem = Source.PhanLoaiKinhNghiem,
                HardSkills = Source.HardSkills, SoftSkills = Source.SoftSkills,
                LinhVuc = Source.LinhVuc, LinhVuc_clean = Source.LinhVuc_clean,
                HocVan = Source.HocVan, HocVan_clean = Source.HocVan_clean,
                HinhThucLamViec = Source.HinhThucLamViec,
                HinhThucLamViec_clean = Source.HinhThucLamViec_clean,
                KieuLamViec_clean = Source.KieuLamViec_clean,
                SoLuongTuyen = Source.SoLuongTuyen,
                SoLuongTuyen_clean = Source.SoLuongTuyen_clean,
                QuyMoCongTy = Source.QuyMoCongTy,
                QuyMoCongTyMin_clean = Source.QuyMoCongTyMin_clean,
                QuyMoCongTyMax_clean = Source.QuyMoCongTyMax_clean,
                QuyMoCongTyTB_clean = Source.QuyMoCongTyTB_clean,
                PhanLoaiQuyMoCongTy = Source.PhanLoaiQuyMoCongTy,
                HanNopHoSo = Source.HanNopHoSo_Date,
                HanNopHoSo_clean = Source.HanNopHoSo_Clean_Date,
                Nguon = Source.Nguon,
                NgayCaoDuLieu = Source.NgayCaoDuLieu_Date,
                NgayXuLyDL = GETDATE()

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                JobHash, LinkBaiTuyenDung, CongTy, CongTy_clean, CongViec, CongViec_clean,
                CapBac, CapBac_clean, ViTri, ViTri_clean, Tinh_Thanh, KhuVuc, Latitude, Longitude,
                MucLuong, MucLuongMin_clean, MucLuongMax_clean, MucLuongTB_clean, KhoangLuong,
                MoTaCongViec, YeuCauUngVien, YeuCauKinhNghiem, YeuCauKinhNghiemMin_clean, 
                YeuCauKinhNghiemMax_clean, YeuCauKinhNghiemTB_clean, PhanLoaiKinhNghiem,
                HardSkills, SoftSkills, LinhVuc, LinhVuc_clean, HocVan, HocVan_clean,
                HinhThucLamViec, HinhThucLamViec_clean, KieuLamViec_clean,
                SoLuongTuyen, SoLuongTuyen_clean, QuyMoCongTy, QuyMoCongTyMin_clean, 
                QuyMoCongTyMax_clean, QuyMoCongTyTB_clean, PhanLoaiQuyMoCongTy,
                HanNopHoSo, HanNopHoSo_clean, Nguon, NgayCaoDuLieu, NgayXuLyDL
            )
            VALUES (
                Source.JobHash, Source.LinkBaiTuyenDung, Source.CongTy, Source.CongTy_clean,
                Source.CongViec, Source.CongViec_clean, Source.CapBac, Source.CapBac_clean,
                Source.ViTri, Source.ViTri_clean, Source.Tinh_Thanh, Source.KhuVuc, Source.Latitude, Source.Longitude,
                Source.MucLuong, Source.MucLuongMin_clean, Source.MucLuongMax_clean, Source.MucLuongTB_clean,
                Source.KhoangLuong, Source.MoTaCongViec, Source.YeuCauUngVien, Source.YeuCauKinhNghiem,
                Source.YeuCauKinhNghiemMin_clean, Source.YeuCauKinhNghiemMax_clean, Source.YeuCauKinhNghiemTB_clean,
                Source.PhanLoaiKinhNghiem, Source.HardSkills, Source.SoftSkills,
                Source.LinhVuc, Source.LinhVuc_clean, Source.HocVan, Source.HocVan_clean,
                Source.HinhThucLamViec, Source.HinhThucLamViec_clean, Source.KieuLamViec_clean,
                Source.SoLuongTuyen, Source.SoLuongTuyen_clean, Source.QuyMoCongTy, Source.QuyMoCongTyMin_clean,
                Source.QuyMoCongTyMax_clean, Source.QuyMoCongTyTB_clean, Source.PhanLoaiQuyMoCongTy,
                Source.HanNopHoSo_Date, Source.HanNopHoSo_Clean_Date, Source.Nguon, Source.NgayCaoDuLieu_Date, GETDATE()
            )

        OUTPUT $action INTO @MergeOutput;

        ------------------------------------------------------------
        -- 4. TỔNG HỢP & GHI LOG
        ------------------------------------------------------------
        -- Đếm số dòng từ JSON đầu vào (Đã lọc)
        SELECT @TotalInputRows = COUNT(*) 
        FROM OPENJSON(@JsonData) 
        WITH (CongViec NVARCHAR(MAX) '$.CongViec')
        WHERE CongViec IS NOT NULL AND LTRIM(RTRIM(CongViec)) <> '';

        -- Đếm kết quả MERGE
        SELECT @InsertedCount = COUNT(*) FROM @MergeOutput WHERE ActionType = 'INSERT';
        SELECT @UpdatedCount = COUNT(*) FROM @MergeOutput WHERE ActionType = 'UPDATE';

        -- Ghi Log Thành Công
        INSERT INTO [dbo].[ETL_Job_Log] (
            [ProcedureName], [StartTime], [EndTime], [DurationSeconds], 
            [Status], [TotalRowsInput], [NewJobsInserted], [OldJobsUpdated]
        )
        VALUES (
            'sp_Import_FactCleanJobs_JSON',
            @StartTime,
            GETDATE(),
            DATEDIFF(MILLISECOND, @StartTime, GETDATE()) / 1000.0,
            'Success',
            @TotalInputRows,
            @InsertedCount,
            @UpdatedCount
        );

    END TRY
    BEGIN CATCH
        ------------------------------------------------------------
        -- 5. XỬ LÝ LỖI
        ------------------------------------------------------------
        SET @ErrorMessage = ERROR_MESSAGE();
        
        -- Ghi Log Thất Bại
        INSERT INTO [dbo].[ETL_Job_Log] (
            [ProcedureName], [StartTime], [EndTime], [DurationSeconds], 
            [Status], [TotalRowsInput], [ErrorMessage]
        )
        VALUES (
            'sp_Import_FactCleanJobs_JSON',
            @StartTime,
            GETDATE(),
            DATEDIFF(MILLISECOND, @StartTime, GETDATE()) / 1000.0,
            'Failed',
            @TotalInputRows,
            @ErrorMessage
        );

        -- Ném lỗi ra ngoài để Python biết
        THROW; 
    END CATCH
END
GO