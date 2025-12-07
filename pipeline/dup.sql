CREATE OR ALTER PROCEDURE [dbo].[sp_Update_FactJobPostings]
AS
BEGIN
    SET NOCOUNT ON; 
    
    -- 1. KHAI BÁO BIẾN LOGGING
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @TotalStagingRows INT = 0;
    DECLARE @InsertedCount INT = 0;
    DECLARE @UpdatedCount INT = 0;
    DECLARE @ErrorMessage NVARCHAR(MAX) = NULL;
    
    -- Biến bảng để hứng kết quả Insert/Update từ lệnh MERGE
    DECLARE @MergeOutput TABLE (ActionType NVARCHAR(20));

    PRINT 'Bat dau Tac vu 2 (Stager -> Fact) kem Logging...';

    BEGIN TRY
        
        -- Đếm tổng số dòng trong Staging trước khi xử lý
        SELECT @TotalStagingRows = COUNT(*) FROM [dbo].[Stg_Jobs_ta];

        -- SỬ DỤNG CTE ĐỂ CHUẨN BỊ DỮ LIỆU
        ;WITH CleanSource AS (
            SELECT 
                *,
                CONVERT(VARCHAR(32), HASHBYTES('MD5', 
                    LOWER(ISNULL(TRIM([CongViec]), '')) + 
                    LOWER(ISNULL(TRIM([CongTy]), '')) + 
                    LOWER(ISNULL(TRIM([ViTri]), '')) 
                ), 2) AS CalculatedHash,
                ROW_NUMBER() OVER(
                    PARTITION BY [LinkBaiTuyenDung] 
                    ORDER BY [NgayCaoDuLieu] DESC
                ) as rn
            FROM [dbo].[Stg_Jobs_ta]
        )

        -- THỰC HIỆN MERGE
        MERGE INTO [dbo].[Fact_JobPostings] AS [Target]
        USING (
            SELECT * FROM CleanSource WHERE rn = 1
        ) AS [Source]
        ON ([Target].[LinkBaiTuyenDung] = [Source].[LinkBaiTuyenDung]) 

        -- TRƯỜNG HỢP 1: CẬP NHẬT
        WHEN MATCHED THEN
            UPDATE SET
                [Target].[JobHash]          = [Source].[CalculatedHash],
                [Target].[CongViec]         = [Source].[CongViec],
                [Target].[ViTri]            = [Source].[ViTri],
                [Target].[YeuCauKinhNghiem] = [Source].[YeuCauKinhNghiem],
                [Target].[MucLuong]         = [Source].[MucLuong],
                [Target].[CapBac]           = [Source].[CapBac],
                [Target].[HinhThucLamViec]  = [Source].[HinhThucLamViec],
                [Target].[CongTy]           = [Source].[CongTy],
                [Target].[QuyMoCongTy]      = [Source].[QuyMoCongTy],
                [Target].[SoLuongTuyen]     = [Source].[SoLuongTuyen],
                [Target].[HocVan]           = [Source].[HocVan],
                [Target].[YeuCauUngVien]    = [Source].[YeuCauUngVien],
                [Target].[MoTaCongViec]     = [Source].[MoTaCongViec],
                [Target].[QuyenLoi]         = [Source].[QuyenLoi],
                [Target].[HanNopHoSo]       = [Source].[HanNopHoSo],
                [Target].[Nguon]            = [Source].[Nguon],
                [Target].[NgayCaoDuLieu]    = [Source].[NgayCaoDuLieu],
                [Target].[LinhVuc]          = [Source].[LinhVuc],
                [Target].[NgayThemVaoHeThong] = GETDATE() -- Cập nhật ngày sửa đổi mới nhất

        -- TRƯỜNG HỢP 2: THÊM MỚI
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                [JobHash], [NgayThemVaoHeThong],
                [CongViec], [ViTri], [YeuCauKinhNghiem], [MucLuong],
                [CapBac], [HinhThucLamViec], [CongTy],
                [QuyMoCongTy], [SoLuongTuyen], [HocVan],
                [YeuCauUngVien], [MoTaCongViec], [QuyenLoi],
                [HanNopHoSo], [LinkBaiTuyenDung], [Nguon], 
                [NgayCaoDuLieu], [LinhVuc]
            )
            VALUES (
                [Source].[CalculatedHash],
                GETDATE(), 
                [Source].[CongViec], [Source].[ViTri], [Source].[YeuCauKinhNghiem], [Source].[MucLuong],
                [Source].[CapBac], [Source].[HinhThucLamViec], [Source].[CongTy],
                [Source].[QuyMoCongTy], [Source].[SoLuongTuyen], [Source].[HocVan],
                [Source].[YeuCauUngVien], [Source].[MoTaCongViec], [Source].[QuyenLoi],
                [Source].[HanNopHoSo], [Source].[LinkBaiTuyenDung], [Source].[Nguon], 
                [Source].[NgayCaoDuLieu], [Source].[LinhVuc]
            )
        
        -- QUAN TRỌNG: HỨNG KẾT QUẢ RA BIẾN BẢNG ĐỂ ĐẾM
        OUTPUT $action INTO @MergeOutput;

        -- Tính toán số lượng Insert và Update
        SELECT @InsertedCount = COUNT(*) FROM @MergeOutput WHERE ActionType = 'INSERT';
        SELECT @UpdatedCount = COUNT(*) FROM @MergeOutput WHERE ActionType = 'UPDATE';

        PRINT 'Da hoan tat UPSERT. Insert: ' + CAST(@InsertedCount AS VARCHAR) + ', Update: ' + CAST(@UpdatedCount AS VARCHAR);

        -- GHI LOG THÀNH CÔNG
        INSERT INTO [dbo].[ETL_Job_Log] (
            [ProcedureName], [StartTime], [EndTime], [DurationSeconds], 
            [Status], [TotalRowsStaging], [NewJobsInserted], [OldJobsUpdated]
        )
        VALUES (
            'sp_Update_FactJobPostings',
            @StartTime,
            GETDATE(),
            DATEDIFF(MILLISECOND, @StartTime, GETDATE()) / 1000.0,
            'Success',
            @TotalStagingRows,
            @InsertedCount,
            @UpdatedCount
        );

    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        
        PRINT '!!! LOI: ' + @ErrorMessage;

        -- GHI LOG THẤT BẠI
        INSERT INTO [dbo].[ETL_Job_Log] (
            [ProcedureName], [StartTime], [EndTime], [DurationSeconds], 
            [Status], [TotalRowsStaging], [ErrorMessage]
        )
        VALUES (
            'sp_Update_FactJobPostings',
            @StartTime,
            GETDATE(),
            DATEDIFF(MILLISECOND, @StartTime, GETDATE()) / 1000.0,
            'Failed',
            @TotalStagingRows,
            @ErrorMessage
        );

        -- Ném lỗi ra ngoài để Python biết mà dừng pipeline nếu cần
        THROW; 
    END CATCH;

    -- Dọn dẹp Staging sau khi đã log xong (chỉ dọn nếu không lỗi fatal làm dừng proc)
    PRINT 'Dang don sach bang Stg_Jobs_ta...';
    TRUNCATE TABLE [dbo].[Stg_Jobs_ta];
    PRINT 'Da don sach Staging.';

END
GO



