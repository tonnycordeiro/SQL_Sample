-- =============================================
-- Author:		Tonny
-- Create date: 4/08/2009
-- Description:	Export registers according to EDI Project
-- =============================================


CREATE PROCEDURE [dbo].[prc_EDImanageStructure]
	@optionDescription varchar(20), 
	@sequentialCodeRegister int
AS
BEGIN
	DECLARE @fieldId int
	DECLARE @quantity int

	DECLARE @errorMessage VARCHAR(400)
	DECLARE @previousPosition INT
	DECLARE @nextPosition INT
	DECLARE @currentPosition INT
	DECLARE @fieldNumber INT
	DECLARE @registerLength INT

	SET NOCOUNT ON;

	IF @optionDescription = 'sequenceFields'
	BEGIN
		set @quantity = 1
		DECLARE CurField CURSOR SCROLL FOR
			SELECT [EDIFLD_ID]
			  FROM [EDI_FIELD]
			where [EDIFLD_REG_seqCode] = @sequentialCodeRegister
			ORDER BY [EDIFLD_num]
		OPEN CurField
		FETCH NEXT FROM CurField INTO @fieldId
		
		WHILE @@FETCH_STATUS = 0
		BEGIN
			UPDATE [EDI_FIELD]
			   SET [EDIFLD_num] = @quantity
			 WHERE [EDIFLD_ID] = @fieldId
			set @quantity = @quantity + 1
			FETCH NEXT FROM CurField INTO @fieldId
		END
		DEALLOCATE CurField
	END
	IF @optionDescription = 'checkInitialPosition'
	BEGIN
		set @errorMessage = ''
		DECLARE CurField CURSOR SCROLL FOR
			SELECT EDIFLD_position, (EDIFLD_lengthIntegerPart + EDIFLD_lengthDescPart + EDIFLD_position) proxPosicao, EDIFLD_num 
			  FROM [EDI_FIELD]
			where [EDIFLD_REG_seqCode] = @sequentialCodeRegister
			ORDER BY [EDIFLD_num]
		OPEN CurField
		FETCH NEXT FROM CurField INTO @currentPosition, @nextPosition, @fieldNumber
		set @previousPosition = 1
		WHILE @@FETCH_STATUS = 0
		BEGIN
			if @currentPosition <> @previousPosition	
			BEGIN
				if @errorMessage <> ''
					set @errorMessage = @errorMessage + ','
				set @errorMessage = @errorMessage + cast(@fieldNumber as varchar(5))
			END
			set @previousPosition = @nextPosition
			FETCH NEXT FROM CurField INTO @currentPosition,@nextPosition, @fieldNumber
		END
		set @registerLength = (SELECT EDIRG_length FROM EDI_REGISTER WHERE EDIRG_seqCode = @sequentialCodeRegister)
		if @registerLength <> @nextPosition-1
		BEGIN
			if @errorMessage <> ''
				set @errorMessage = @errorMessage + ','
			set @errorMessage = @errorMessage + cast(@fieldNumber as varchar(5))
		END
		SELECT @errorMessage AS erro
	DEALLOCATE CurField
	END
END
GO


/****** Object:  UserDefinedFunction [dbo].[EDIparseDataToExportTXT]    Script Date: 01/14/2011 14:36:51 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[EDIparseDataToExportTXT] 
(
	@dataValue varchar(250) = NULL,
	@dataType char(1),
	@newLength int,
	@minLength int,
	@statusExportField CHAR(1),
	@isFixedLength bit,
	@enableChanging bit,
	@compositionFieldsType int
)
RETURNS VARCHAR(250)
AS
BEGIN
	DECLARE @aux VARCHAR(100);
	DECLARE @parsedData VARCHAR(250);
	DECLARE @characterFillingQuantity INT;
	DECLARE @COMPOSITION_TYPE_POSITIONAL INT = 1;
	DECLARE @COMPOSITION_TYPE_BY_SEPARATORS INT = 2;
	
	if @dataValue is NULL
		set @dataValue = ''
	set @parsedData = @dataValue
	if Len(replace(@dataValue,' ','_')) > 0
	 BEGIN
		IF @isFixedLength = 1  -- the positional templates have fixed length
			SET @minLength = @newLength
		if Len(replace(@dataValue,' ','_')) < @minLength
			if @dataType = 'A'
				set @parsedData = @dataValue + Replicate(' ',@newLength-Len(replace(@dataValue,' ','_')))
			else
				set @parsedData = Replicate('0',@newLength-Len(replace(@dataValue,' ','_'))) + @dataValue
		else
			if Len(replace(@dataValue,' ','_')) > @newLength
				if @dataType = 'A'
					set @parsedData = Left(@dataValue,@newLength)
				else
					begin
							if (CHARINDEX(' ',@dataValue)>0)
							 begin
								set @parsedData = LEFT(replace(@dataValue,' ',''),@newLength)
								set @characterFillingQuantity = @newLength-Len(@novoDado)
								if (@characterFillingQuantity)<0
									set @characterFillingQuantity = 0
								set @parsedData = Replicate('0',@characterFillingQuantity) + @parsedData
							 end
							else
								set @parsedData = Right(@dataValue,@newLength)
					end
	 END
	else
	 BEGIN
		IF	(@statusExportField = 'M' AND @compositionFieldsType = COMPOSITION_TYPE_BY_SEPARATORS) OR @compositionFieldsType = COMPOSITION_TYPE_POSITIONAL
		 BEGIN
			IF @minLength IS NOT NULL
				SET @newLength = @minLength 	
			if @dataType = 'A'
				set @parsedData = Replicate(' ',@newLength)
			else
				set @parsedData = Replicate('0',@newLength)	
		 END
	 END
	
	if @enableChanging = 1
		SET @aux = (SELECT TOP 1 EDIFRV_replacementValue FROM EDI_FIELD_REPLACE_VALUE 
					WHERE EDIFRV_originalValue= @parsedData ORDER BY EDIFRV_replacementValue DESC)
	if @aux is not NULL
		set @parsedData = dbo.EDIparseDataToExportTXTwithSep(@aux,@dataType,@newLength,@minLength,@statusExportField,@isFixedLength,null,@compositionFieldsType)
	RETURN @parsedData

END
GO


CREATE PROCEDURE [dbo].[prc_ExportFileEDI]
	@userCode int = NULL,
	@exportMonth int = NULL,
	@register varchar(MAX) = NULL, 
	@seqCodeReg int = NULL,
	@fileNameTXT varchar(100),
	@optionDescription varchar(30)
AS
BEGIN
	DECLARE @fieldId int
	DECLARE @fieldLength int
	DECLARE @pos int
	DECLARE @dataValue varchar(MAX)
	DECLARE @isFileEmpty bit
	DECLARE @fileStatus int
	DECLARE @fileGroupId int

	SET NOCOUNT ON;

	if @optionDescription = 'insertFile'
	BEGIN
		set @fileStatus = (SELECT EDIFG_status FROM EDI_filegroup WHERE EDIFG_fileName = @fileNameTXT)
		SET @isFileEmpty = 1

		IF @fileStatus IS NULL
		BEGIN
			INSERT INTO [EDI_filegroup]
					   ([EDIFG_fileName]
					   ,[EDIFG_status]
					   ,[EDIFG_user]
					   ,[EDIFG_exportMonth])
				 VALUES
					   (@fileNameTXT
					   ,2
					   ,@userCode
					   ,@exportMonth)
			SET @isFileEmpty = 0
		END
		ELSE
			IF @fileStatus = 1
			BEGIN
				UPDATE [EDI_filegroup]
					SET [EDIFG_fileName] = @fileNameTXT
						,[EDIFG_status] = 2
						,[EDIFG_user] = @userCode
						,[EDIFG_exportMonth] = @exportMonth
				WHERE EDIFG_fileName = @fileNameTXT

				SET @isFileEmpty = 0
			END
	END
	if @optionDescription = 'splitRegister'
	BEGIN
		
		SET @fileGroupId = (SELECT EDIFG_id FROM EDI_filegroup WHERE EDIFG_fileName = @fileNameTXT)
		DECLARE CurReg CURSOR FAST_FORWARD FOR
			Select	EDIFLD_ID,
					(EDIFLD_lengthIntegerPart + EDIFLD_lengthDescPart) fieldLength,
					EDIFLD_position FROM vw_EDI_ExportFile
				WHERE EDIRG_seqCode = @seqCodeReg
				ORDER BY EDIFLD_position

		OPEN CurReg
		FETCH NEXT FROM CurReg INTO @fieldId, @fieldLength, @pos
		
		WHILE @@FETCH_STATUS = 0
		BEGIN
			set @dataValue = Substring(@register,@pos,@fieldLength)

			INSERT INTO [EDI_DATA_FIELD]
					   ([EDIDTFLD_DATA]
					   ,[EDIDTFLD_FIELD_ID]
					   ,[EDIDTFLD_FILEGROUP_ID])
				 VALUES
					   (@dataValue
					   ,@fieldId
					   ,@fileGroupId)
			
			FETCH NEXT FROM CurReg INTO @fieldId,@fieldLength, @pos
		END
		DEALLOCATE CurReg
	END

END
GO



/****** Object:  StoredProcedure [dbo].[prc_EDI_listExportRegister]    Script Date: 01/14/2011 14:36:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[prc_EDI_listExportRegister]
(
	@templateCode int,
	@searchOrder varchar(8000),
	@fromWhereSearch varchar(8000) = ''
)
AS
BEGIN
	declare @fromWhereSearchProc varchar(8000)
	declare @columnNameView varchar(100)
	declare @columnNameLinkedTable varchar(100)
	declare @SQLfirstSelection varchar(7)
	declare @extraCondition varchar(2000)
	declare @columnNameQuery varchar(8000)
	declare @strTemp varchar(8000)
	declare @conditionalOrders varchar(8000)
	declare @SQLWhereOrdCond varchar(8000)
	declare @columnValueLinkedTable varchar(250)
	
	
	declare @seqCodeRegExport int
	declare @tableId int

	declare @tableName varchar(50)

	declare @compositionFieldsType int
	declare @separator char(1)

	declare @addOrder bit
	declare @endOrdersConditional bit
	declare @temRegExport bit

	declare @countContent int
	declare @countDependents int
	declare @orderExport int
	declare @orderExportDep int

	declare @isOneRegister bit

	DECLARE @COMPOSITION_TYPE_POSITIONAL INT = 1;
	DECLARE @COMPOSITION_TYPE_BY_SEPARATORS INT = 2;

	IF OBJECT_ID('tempdb..##rowsExport') IS NOT NULL 
		DROP TABLE ##rowsExport
	CREATE TABLE ##rowsExport (rowValue varchar(8000))
	set @fromWhereSearchProc = @fromWhereSearch
	set @fromWhereSearch = replace(@fromWhereSearch,'<+>',Char(39))

	DECLARE CurModelo CURSOR FAST_FORWARD FOR
		SELECT EDIMD_LinkedTable, EDIMD_separator FROM [EDI_MODELO] where EDIMD_id=@templateCode
	OPEN CurModelo
	FETCH NEXT FROM CurModelo INTO @tableId, @separator
	CLOSE CurModelo
	DEALLOCATE CurModelo

	set @tableName = (SELECT NAME FROM sys.objects where OBJECT_ID=@tableId)
	set @compositionFieldsType = COMPOSITION_TYPE_POSITIONAL
	if @separator is not null
		set @compositionFieldsType = COMPOSITION_TYPE_BY_SEPARATORS

	DECLARE CurOrder CURSOR FAST_FORWARD FOR

			SELECT distinct orderExport FROM vw_EDI_StructureExport 
			WHERE idTemplateExport = @templateCode AND typeRegExport = 'C' AND idLinkedTable = @tableId
			AND orderExport IS NOT NULL ORDER BY orderExport

	OPEN CurOrder
	FETCH NEXT FROM CurOrder INTO @orderExport

	--it will be included because its registers are linked to data in DB
	set @conditionalOrders = ''

	WHILE @@FETCH_STATUS = 0
	BEGIN
		set @addOrder = 0 --firstly the table is empty	

		DECLARE CurCol CURSOR FAST_FORWARD FOR
			SELECT (SELECT NAME FROM sys.COLUMNS where OBJECT_ID=vExp.idLinkedTable and column_id=vExp.idlLinkedColumn) coluna
					FROM vw_EDI_StructureExport vExp WHERE idLinkedTable IS NOT NULL AND orderExport = @orderExport
		OPEN CurCol
		FETCH NEXT FROM CurCol INTO @columnNameView
		
		if @@FETCH_STATUS = 0
			set @endOrdersConditional = 0
		else
			set @endOrdersConditional = 1
			
		WHILE @endOrdersConditional = 1
		BEGIN
			set @columnNameQuery = @tableName + '.' + @columnNameView

			IF OBJECT_ID('tempdb..##ONEVALUE') IS NOT NULL 
				DROP TABLE ##ONEVALUE
			SET @strTemp = '(Select top 1 CAST((' + @columnNameQuery + 
						   ') AS VARCHAR(250)) valor INTO ##ONEVALUE ' + @searchOrder + ' AND ' + @columnNameQuery + ' IS NOT NULL)'

			EXECUTE(@strTemp)
			set @countContent = (SELECT COUNT(valor) FROM ##ONEVALUE)
			drop table ##ONEVALUE

			FETCH NEXT FROM CurCol INTO @columnNameView
			set @endOrdersConditional = 0
			if @@FETCH_STATUS = 0
				set @endOrdersConditional = 0
			else
				set @endOrdersConditional = 1
			
			if @countContent>0
			BEGIN
				set @addOrder = 1
				set @endOrdersConditional = 1
			END					
		END
		if @addOrder = 1
			set @conditionalOrders = @conditionalOrders + ',' + cast(@orderExport as varchar(3))
		
		close CurCol
		deallocate CurCol
		FETCH NEXT FROM CurOrder INTO @orderExport
	END
	if len(@conditionalOrders)>0
		set @conditionalOrders = stuff(@conditionalOrders,1,1,'')

	set @SQLWhereOrdCond = ''
	if @conditionalOrders <> '' and @conditionalOrders is not null
		set @SQLWhereOrdCond = ' OR orderExport in (' + @conditionalOrders + ')'

	IF OBJECT_ID('tempdb..##tabOrders') IS NOT NULL 
		DROP TABLE ##tabOrders
	set @strTemp =	'SELECT distinct orderExport, seqCodeRegExport into ##tabOrders FROM vw_EDI_StructureExport WHERE idTemplateExport = ' +
					cast(@templateCode as varchar(6)) + ' AND (typeRegExport = ' + Char(39) + 'M' + Char(39) + @SQLWhereOrdCond + ')' +
					' AND orderExport IS NOT NULL ORDER BY orderExport '
	EXECUTE(@strTemp)

	close CurOrder
	deallocate CurOrder

	DECLARE CurOrder CURSOR FAST_FORWARD FOR
			SELECT * from ##tabOrders
	OPEN CurOrder
	FETCH NEXT FROM CurOrder INTO @orderExport,@seqCodeRegExport

	WHILE @@FETCH_STATUS = 0
	BEGIN

		DECLARE CurDepend CURSOR SCROLL FOR		

			Select distinct vwExt.orderExport, 
			(select top 1 (SELECT NAME FROM sys.COLUMNS where OBJECT_ID=vwInt.idLinkedTable and column_id=vwInt.idlLinkedColumn)
			from vw_EDI_StructureExport as vwInt where vwInt.idFieldExport = vwExt.idDependentField)
			as columnNameLinkedTable, (Case when ISNULL(registerCount,0)=1 then 1 else 0 end) isOneRegister from vw_EDI_StructureExport as vwExt 
			where vwExt.seqCodeRegDependent = @seqCodeRegExport
			and vwExt.idDependentField is not null ORDER BY orderExport

		OPEN CurDepend
		FETCH NEXT FROM CurDepend INTO @orderExportDep,@columnNameLinkedTable, @isOneRegister

		set @countDependents = @@Cursor_Rows

		if @countDependents = 0 -- if there are not other dependent registers from current
		BEGIN
			SET @strTemp =	'prc_EDIcreateExportRegister @templateCode = ' + cast(@templateCode as varchar(20)) +
							', @orderExport = ' + cast(@orderExport AS VARCHAR(3)) + ',@fromWhereSearch =' + Char(39) + @fromWhereSearchProc + Char(39) + 
							', @tableId = ' + CAST(@tableId AS VARCHAR(20))+ 
							', @compositionFieldsType =' + CAST(@compositionFieldsType AS CHAR(1)) +
							',@separator=' + Char(39) + @separator + Char(39) + ',@tableName=' + Char(39) + @tableName + Char(39)

			execute(@strTemp)
		END
		else
		BEGIN
			SET @columnNameQuery = @tableName + '.' + @columnNameLinkedTable
			IF OBJECT_ID('tempdb..##ONEVALUE') IS NOT NULL 
				DROP TABLE ##ONEVALUE
			SET @strTemp = 'Select distinct ' + @columnNameQuery + ' valor INTO ##ONEVALUE ' + @fromWhereSearch

			EXECUTE(@strTemp)

			DECLARE CurPart CURSOR FAST_FORWARD FOR		
				select * from ##ONEVALUE
			OPEN CurPart
			FETCH NEXT FROM CurPart INTO @columnValueLinkedTable

			WHILE @@FETCH_STATUS = 0
			BEGIN
				SET @extraCondition = ' and ' + @columnNameLinkedTable + ' = ' + @columnValueLinkedTable
				set @strTemp =	'prc_EDIcreateExportRegister @templateCode = ' + cast(@templateCode AS VARCHAR(3)) +
								', @orderExport = ' + cast(@orderExport AS VARCHAR(3)) + ',@fromWhereSearch =' + Char(39) + @fromWhereSearchProc + @extraCondition + Char(39) +
								', @isPrintable = 1 ' +
								', @tableId = ' + CAST(@tableId AS VARCHAR(20))+ 
								', @compositionFieldsType =' + CAST(@compositionFieldsType AS CHAR(1)) +
								',@separator=' + Char(39) + @separator + Char(39) + ',@tableName=' + Char(39) + @tableName + Char(39)

				execute(@strTemp)
				FETCH FIRST FROM CurDepend INTO @orderExportDep, @columnNameLinkedTable, @isOneRegister					
				WHILE @@FETCH_STATUS = 0
				BEGIN
					set @strTemp =	'prc_EDIcreateExportRegister @templateCode = ' + cast(@templateCode AS VARCHAR(20)) +
									', @orderExport = ' + cast(@orderExportDep AS VARCHAR(3)) + ',@fromWhereSearch =' + Char(39) + @fromWhereSearchProc + @extraCondition + Char(39) +
									', @isPrintable = ' + CAST(@isOneRegister AS VARCHAR(1)) +
									', @tableId = ' + CAST(@tableId AS VARCHAR(20))+ 
									', @compositionFieldsType =' + CAST(@compositionFieldsType AS CHAR(1)) +
									',@separator=' + Char(39) + @separator + Char(39) + ',@tableName=' + Char(39) + @tableName + Char(39)

					execute(@strTemp)

					FETCH NEXT FROM CurDepend INTO @orderExportDep,@columnNameLinkedTable, @isOneRegister
				END
				FETCH FIRST FROM CurDepend INTO @orderExportDep,@columnNameLinkedTable, @isOneRegister
				FETCH NEXT FROM CurPart INTO @columnValueLinkedTable
			END
			drop table ##ONEVALUE
			CLOSE CURPART
			DEALLOCATE CURPART

		END
		While @countDependents > 0
		BEGIN
			FETCH NEXT FROM CurOrder INTO @orderExport,@seqCodeRegExport
			set @countDependents = @countDependents-1
		END

		CLOSE CurDepend
		DEALLOCATE CurDepend
		
		FETCH NEXT FROM CurOrder INTO @orderExport,@seqCodeRegExport
	END

	drop table ##tabOrders

	close CurOrder
	deallocate CurOrder


END
GO
/****** Object:  StoredProcedure [dbo].[prc_EDIcreateExportRegister]    Script Date: 01/14/2011 14:36:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[prc_EDIcreateExportRegister]
(
	@templateCode int,
	@orderExport int,
	@fromWhereSearch varchar(8000) = '',
	@isPrintable bit = 0,
	@tableId int,
	@separator char(1),
	@compositionFieldsType int,
	@tableName varchar(50)
)
AS
BEGIN
	declare @columnName varchar(8)
	declare @SQLfirstSelection varchar(7)
	declare @columnNameQuery varchar(8000)
	declare @allColumNames varchar(8000)
	declare @nullValuesForAllColumns varchar(8000)
	declare @columnNameGroup varchar(8000)	
	declare @columnNamesLinkedData varchar(8000)
	declare @columnNamesInsertLinkedData varchar(8000)
	declare @strCriateTable varchar(8000) 
	declare @strInsertRow varchar(8000) 
	declare @columnNameReg varchar(8000) 
	declare @valuesWithoutDataFirstRow varchar(8000) 
	declare @valuesWithDataFirstRow varchar(8000) 
	declare @regWithOneEvent varchar(8000)
	declare @strTemp varchar(8000)
	declare @SQLSelect varchar(8000)
	declare @dataToInsertInTable varchar(8000)
	
	declare @idFile int
	declare @idTemplateExport int
	declare @idFieldExport int
	declare @idFieldImport int
	declare @seqCodeRegImport int	
	declare @seqCodeRegExport int
	declare @exportFieldLength int
	declare @minLengthExportField int
	declare @idLinkedTable int
	declare @idlLinkedColumn int
	declare @queryTypeSQL int

	declare @tpDataExport char(1) 
	declare @statusExportField char(1)
	declare @vlFieldExport varchar(250)
	declare @idRegExport varchar(50)
	declare @queryColumnSQL varchar(8000)

	declare @replacementRows varchar(5)
	
	declare @isFixedLength bit
	declare @enableChanging bit

	DECLARE @COMPOSITION_TYPE_POSITIONAL INT = 1;
	DECLARE @COMPOSITION_TYPE_BY_SEPARATORS INT = 2;
	
	set @fromWhereSearch = replace(@fromWhereSearch,'<+>',Char(39))
	
	set @SQLfirstSelection = ''
	if @isPrintable=1
		set @SQLfirstSelection = ' top 1 '

	DECLARE CurField CURSOR SCROLL FOR
		SELECT [idTemplateExport]
			  ,[idRegExport]
			  ,[idFieldExport]
			  ,[vlFieldExport]
			  ,[tpDataExport]
			  ,[lengthFieldExport]
			  ,[minLengthExportField]
			  ,[seqCodeRegExport]
			  ,[idLinkedTable]
			  ,[idlLinkedColumn]
			  ,[isFixedLength]
			  ,[statusExportField]
			  ,[queryTypeSQL]
			  ,[queryColumnSQL]
			  ,[enableChanging]
		  FROM [vw_EDI_StructureExport]
		  where [idTemplateExport] = @templateCode and orderExport = @orderExport
	OPEN CurField
	FETCH NEXT FROM CurField INTO @idTemplateExport, @idRegExport, @idFieldExport, @vlFieldExport, @tpDataExport,
								  @exportFieldLength, @minLengthExportField, @seqCodeRegExport, @idLinkedTable, @idlLinkedColumn,
								  @isFixedLength,@statusExportField,@queryTypeSQL,@queryColumnSQL,@enableChanging
	
	--CREATE TABLE
	IF OBJECT_ID('tempdb..##EDIREGISTER') IS NOT NULL 
		DROP TABLE ##EDIREGISTER 
	CREATE TABLE ##EDIREGISTER (id int IDENTITY(1,1))

	set @allColumNames = ''
	WHILE @@FETCH_STATUS = 0
	BEGIN
		set @columnName = '_' + cast(@idFieldExport as varchar(6))
		SET @strCriateTable = 'ALTER TABLE ##EDIREGISTER ADD ' + @columnName + ' VARCHAR(1000) NULL'
		
		if @allColumNames = ''
		BEGIN
			SET @allColumNames = @columnName
			SET @nullValuesForAllColumns = 'NULL'
		END	
		else
		BEGIN
			set @allColumNames = @allColumNames + ',' + @columnName
			set @nullValuesForAllColumns = @nullValuesForAllColumns + ',NULL'
		END
		execute(@strCriateTable)
		IF @vlFieldExport is null and @idlLinkedColumn is null 
		 BEGIN
			while @vlFieldExport is null and @idlLinkedColumn is null and @@FETCH_STATUS = 0
				FETCH NEXT FROM CurField INTO @idTemplateExport, @idRegExport, @idFieldExport, @vlFieldExport, @tpDataExport,
											  @exportFieldLength, @minLengthExportField, @seqCodeRegExport, @idLinkedTable, @idlLinkedColumn,
											  @isFixedLength,@statusExportField,@queryTypeSQL,@queryColumnSQL,@enableChanging
		 END	
		ELSE
			FETCH NEXT FROM CurField INTO @idTemplateExport, @idRegExport, @idFieldExport, @vlFieldExport, @tpDataExport,
										  @exportFieldLength, @minLengthExportField, @seqCodeRegExport, @idLinkedTable, @idlLinkedColumn,
										  @isFixedLength,@statusExportField,@queryTypeSQL,@queryColumnSQL,@enableChanging
	END

	--CREATE TEMPLATE
	/*
	SELECT TOP 1 * INTO ##MOLDE_VAZIOS FROM ##EDIREGISTER
	ALTER TABLE ##MOLDE_VAZIOS DROP COLUMN ID
	DELETE FROM ##EDIREGISTER
	*/

	set @columnNamesLinkedData = ''
	set @columnNamesInsertLinkedData = ''

	DECLARE CurLinkedData CURSOR FAST_FORWARD FOR
		SELECT [idTemplateExport]
			  ,[idRegExport]
			  ,[idFieldExport]
			  ,[vlFieldExport]
			  ,[tpDataExport]
			  ,[lengthFieldExport]
			  ,[minLengthExportField]
			  ,[seqCodeRegExport]
			  ,[idLinkedTable]
			  ,[idlLinkedColumn]
			  ,[isFixedLength]
			  ,[statusExportField]
			  ,[queryTypeSQL]
			  ,[queryColumnSQL]
			  ,[enableChanging]
		  FROM [vw_EDI_StructureExport]
		  where [idTemplateExport] = @templateCode and orderExport = @orderExport AND [idLinkedTable] = @tableId and ([queryTypeSQL] is NULL OR [queryTypeSQL] <> 1)
	OPEN CurLinkedData
	FETCH NEXT FROM CurLinkedData INTO @idTemplateExport, @idRegExport, @idFieldExport, @vlFieldExport, @tpDataExport,
								  @exportFieldLength, @minLengthExportField, @seqCodeRegExport, @idLinkedTable, @idlLinkedColumn,
								  @isFixedLength,@statusExportField,@queryTypeSQL,@queryColumnSQL,@enableChanging


	IF @@FETCH_STATUS = 0
	 BEGIN
		
		set @columnNameQuery = @tableName + '.' + (SELECT NAME FROM sys.COLUMNS where OBJECT_ID=@tableId and column_id=@idlLinkedColumn)
		IF @queryTypeSQL = 2
			SET @columnNameQuery = '(' + replace(@queryColumnSQL,'|x|',@columnNameQuery) + ')'

		set @columnNamesInsertLinkedData = '_' + cast(@idFieldExport as varchar(6))
		set @columnNamesLinkedData = '( CHAR(39) + dbo.EDIparseDataToExportTXT(LTRIM(RTRIM(ISNULL(' + @columnNameQuery +
								',' +char(39)+char(39)+ '))),' + char(39) + @tpDataExport + char(39) + ',' + 
								cast(@exportFieldLength as varchar(4)) + ',' + ISNULL(cast(@minLengthExportField as varchar(4)),'NULL') + 
								',' + char(39) + @statusExportField + char(39) + ',' + cast(@isFixedLength as varchar(1)) + ',' + 
								cast(@enableChanging as varchar(1)) + ',' + cast(@compositionFieldsType as varchar(1)) + ') + CHAR(39) ' 


		FETCH NEXT FROM CurLinkedData INTO @idTemplateExport, @idRegExport, @idFieldExport, @vlFieldExport, @tpDataExport,
									  @exportFieldLength, @minLengthExportField, @seqCodeRegExport, @idLinkedTable, @idlLinkedColumn,
									  @isFixedLength,@statusExportField,@queryTypeSQL,@queryColumnSQL,@enableChanging
	 END

	WHILE @@FETCH_STATUS = 0
	BEGIN
		set @columnNameQuery = @tableName + '.' + (SELECT NAME FROM sys.COLUMNS where OBJECT_ID=@tableId and column_id=@idlLinkedColumn)
		IF @queryTypeSQL = 2
			SET @columnNameQuery = '(' + replace(@queryColumnSQL,'|x|',@columnNameQuery) + ')'
		set @columnNamesInsertLinkedData = @columnNamesInsertLinkedData + ',_' + cast(@idFieldExport as varchar(6))
		set @columnNamesLinkedData = @columnNamesLinkedData +
								' + CHAR(44) + CHAR(39) + dbo.EDIparseDataToExportTXT(LTRIM(RTRIM(ISNULL(' + @columnNameQuery +
								',' +char(39)+char(39)+ '))),' + char(39) + @tpDataExport + char(39) + ',' + 
								cast(@exportFieldLength as varchar(4)) + ',' + ISNULL(cast(@minLengthExportField as varchar(4)),'NULL') + 
								',' + char(39) + @statusExportField + char(39) + ',' + cast(@isFixedLength as varchar(1)) + ',' + 
								cast(@enableChanging as varchar(1)) + ',' + cast(@compositionFieldsType as varchar(1)) + ') + CHAR(39) ' 


		FETCH NEXT FROM CurLinkedData INTO @idTemplateExport, @idRegExport, @idFieldExport, @vlFieldExport, @tpDataExport,
									  @exportFieldLength, @minLengthExportField, @seqCodeRegExport, @idLinkedTable, @idlLinkedColumn,
									  @isFixedLength,@statusExportField,@queryTypeSQL,@queryColumnSQL,@enableChanging
	END

	IF @columnNamesLinkedData <> ''
		SET @columnNamesLinkedData = @columnNamesLinkedData + ') dataInsTable'

	-- FILL TABLE
	IF @columnNamesLinkedData <> ''
	 BEGIN
		IF OBJECT_ID('tempdb..##tabDataToInsert') IS NOT NULL 
			DROP TABLE ##tabDataToInsert 
		SET @SQLSelect = 'SELECT ' + @SQLfirstSelection + @columnNamesLinkedData + ' INTO ##tabDataToInsert ' + @fromWhereSearch 
		
		exec(@SQLSelect)
		DECLARE CurFill CURSOR FAST_FORWARD FOR
			SELECT * FROM ##tabDataToInsert
		OPEN CurFill

		FETCH NEXT FROM CurFill INTO @dataToInsertInTable

		WHILE @@FETCH_STATUS = 0
		BEGIN

			set @strTemp = 'INSERT INTO ##EDIREGISTER(' + @columnNamesInsertLinkedData + ')VALUES (' +
							@dataToInsertInTable + ')'
			EXEC(@strTemp)
			FETCH NEXT FROM CurFill INTO @dataToInsertInTable
		END
		CLOSE CurFill
		DEALLOCATE CurFill
	 END
	ELSE
	 BEGIN
		set @strTemp = 'INSERT INTO ##EDIREGISTER(' + @allColumNames + ')VALUES (' +
						@nullValuesForAllColumns + ')'
		EXEC(@strTemp)
	 END

	--Fill table with white spaces or zeros:
	SET @columnNameReg = ''
	FETCH FIRST FROM CurField INTO @idTemplateExport, @idRegExport, @idFieldExport, @vlFieldExport, @tpDataExport,
								  @exportFieldLength, @minLengthExportField, @seqCodeRegExport, @idLinkedTable, @idlLinkedColumn,
								  @isFixedLength,@statusExportField,@queryTypeSQL,@queryColumnSQL,@enableChanging

	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @columnNameReg <> ''
			set @columnNameReg = @columnNameReg + ','
		set @columnName = '_' + cast(@idFieldExport as varchar(6))
		set @columnNameReg = @columnNameReg + @columnName
		
		IF @vlFieldExport is null and @idlLinkedColumn is null and @queryTypeSQL is null --campos que serão preenchidos com zeros ou vazios
		 BEGIN
			set @valuesWithoutDataFirstRow = char(39)
			while @vlFieldExport is null and @idlLinkedColumn is null and @@FETCH_STATUS = 0
			 BEGIN
				set @valuesWithoutDataFirstRow = @valuesWithoutDataFirstRow + dbo.EDIparseDataToExportTXT(null,@tpDataExport,@exportFieldLength,@minLengthExportField,@statusExportField,@isFixedLength,null,@compositionFieldsType) --campos são aglutinados
				if @compositionFieldsType = COMPOSITION_TYPE_BY_SEPARATORS
					set @valuesWithoutDataFirstRow = @valuesWithoutDataFirstRow + @separator

				FETCH NEXT FROM CurField INTO @idTemplateExport, @idRegExport, @idFieldExport, @vlFieldExport, @tpDataExport,
											  @exportFieldLength, @minLengthExportField, @seqCodeRegExport, @idLinkedTable, @idlLinkedColumn,
											  @isFixedLength,@statusExportField,@queryTypeSQL,@queryColumnSQL,@enableChanging
			 END
			-- extracting the last separator from dataset of empty data
			if @compositionFieldsType = COMPOSITION_TYPE_BY_SEPARATORS
				set @valuesWithoutDataFirstRow = Stuff(@valuesWithoutDataFirstRow,Len(replace(@valuesWithoutDataFirstRow,' ','_')),1,'')
			set @valuesWithoutDataFirstRow = @valuesWithoutDataFirstRow + char(39)
			SET @strTemp = 'UPDATE ##EDIREGISTER SET ' + @columnName + '=' + @valuesWithoutDataFirstRow
			exec(@strTemp)
		 END
		ELSE
		 BEGIN
			IF @queryTypeSQL = 3 -- function executed in SQL no linked to tables
			 BEGIN
				IF OBJECT_ID('tempdb..##ONEVALUE') IS NOT NULL 
					DROP TABLE ##ONEVALUE
				SET @strTemp = '(SELECT CAST((' + @queryColumnSQL + 
							   ') AS VARCHAR(250)) valor INTO ##ONEVALUE ' + @fromWhereSearch + ')'
				EXECUTE(@strTemp)
				SET @valuesWithDataFirstRow = char(39) + (SELECT TOP 1 VALOR FROM ##ONEVALUE) + char(39)
				drop table ##ONEVALUE

				set @valuesWithDataFirstRow = '(' + dbo.EDIparseDataToExportTXT(@valuesWithDataFirstRow,@tpDataExport,@exportFieldLength,@minLengthExportField,@statusExportField,@isFixedLength,null,@compositionFieldsType) + ')'
				SET @strTemp = 'UPDATE ##EDIREGISTER SET ' + @columnName + '=' + @valuesWithDataFirstRow
				exec(@strTemp)
			 END
			ELSE
				IF @vlFieldExport is not null
				 BEGIN
					set @valuesWithDataFirstRow = Char(39) + dbo.EDIparseDataToExportTXT(@vlFieldExport,@tpDataExport,@exportFieldLength,@minLengthExportField,@statusExportField,@isFixedLength,null,@compositionFieldsType) + Char(39)
					SET @strTemp = 'UPDATE ##EDIREGISTER SET ' + @columnName + '=' + @valuesWithDataFirstRow
					exec(@strTemp)
				 END
			ELSE
				IF @queryTypeSQL = 1
				 BEGIN

					set @columnNameQuery = @tableName + '.' + (SELECT NAME FROM sys.COLUMNS where OBJECT_ID=@tableId and column_id=@idlLinkedColumn)
					SET @columnNameQuery = '(' + replace(@queryColumnSQL,'|x|',@columnNameQuery) + ')'
					set @columnNamesInsertLinkedData = '_' + cast(@idFieldExport as varchar(6))
					
					set @columnNameGroup = '( CHAR(39) + dbo.EDIparseDataToExportTXT(LTRIM(RTRIM(ISNULL(' + @columnNameQuery +
											',' +char(39)+char(39)+ '))),' + char(39) + @tpDataExport + char(39) + ',' + 
											cast(@exportFieldLength as varchar(4)) + ',' + ISNULL(cast(@minLengthExportField as varchar(4)),'NULL') + 
											',' + char(39) + @statusExportField + char(39) + ',' + cast(@isFixedLength as varchar(1)) + ',' + 
											cast(@enableChanging as varchar(1)) + ',' + cast(@compositionFieldsType as varchar(1)) + ') + CHAR(39) ) VALOR' 
					IF OBJECT_ID('tempdb..##ONEVALUE') IS NOT NULL 
						DROP TABLE ##ONEVALUE
					SET @SQLSelect = 'SELECT ' + @columnNameGroup + ' INTO ##ONEVALUE ' + @fromWhereSearch
					EXECUTE(@SQLSelect)
					SET @dataToInsertInTable = (SELECT TOP 1 VALOR FROM ##ONEVALUE)
					drop table ##ONEVALUE

					SET @strTemp = 'UPDATE ##EDIREGISTER SET ' + @columnNamesInsertLinkedData + '=' + @dataToInsertInTable
					exec(@strTemp)
				 END

			FETCH NEXT FROM CurField INTO @idTemplateExport, @idRegExport, @idFieldExport, @vlFieldExport, @tpDataExport,
										  @exportFieldLength, @minLengthExportField, @seqCodeRegExport, @idLinkedTable, @idlLinkedColumn,
										  @isFixedLength,@statusExportField,@queryTypeSQL,@queryColumnSQL,@enableChanging
		 END
	END

	CLOSE CurField
	DEALLOCATE CurField	

	CLOSE CurLinkedData
	DEALLOCATE CurLinkedData	

	set @replacementRows = '+'
	if @compositionFieldsType = COMPOSITION_TYPE_BY_SEPARATORS
		set @replacementRows = '+' + Char(39) + @separator + Char(39) + '+'

	IF OBJECT_ID('tempdb..##rowsExport') IS NOT NULL 
		DROP TABLE ##rowsExport
	set @strTemp = 'INSERT INTO ##rowsExport (rowValue) SELECT ' + replace(@columnNameReg,',',@substLinhas) + ' FROM ##EDIREGISTER '
	execute(@strTemp)

	DROP TABLE ##EDIREGISTER
	
	IF @columnNamesLinkedData <> ''
		DROP TABLE ##tabDataToInsert

END
GO
