
# CALL purge_table(schema_name,table_name,date_column,days_of_retention,chunksize_in_rows);

DROP PROCEDURE purge_table;
DELIMITER //
CREATE PROCEDURE purge_table (IN pSchema CHAR(64), IN pTable char(64), IN pTimeColumn char(64), IN pRetentionTimeDays int, IN pChunkSize int)
       BEGIN
         SELECT pSchema,pTable,pTimeColumn,pRetentionTimeDays,pChunkSize;
         SET @sql_del=CONCAT('DELETE FROM ' , pSchema , '.' , pTable , ' WHERE ' , pTimeColumn , '< NOW() - INTERVAL ' , pRetentionTimeDays , ' DAY LIMIT ' , pChunkSize);
         SET @sql_sel=CONCAT('SELECT COUNT(*) INTO @records_left FROM ' , pSchema , '.' , pTable , ' WHERE ' , pTimeColumn , '< NOW() - INTERVAL ' , pRetentionTimeDays, ' DAY');
         SELECT @sql_del;
         SELECT @sql_sel;
         PREPARE stmt_delete FROM @sql_del;
         PREPARE stmt_select FROM @sql_sel;
         SET @records_left=1;
         SET @count=1;
         WHILE @records_left>0 DO
                SELECT CONCAT('Deleting Chunk: ',@count);
                EXECUTE stmt_delete;
                EXECUTE stmt_select;
                SELECT @records_left AS 'Records Left', SLEEP(3) AS SomePause;
		SET @count=@count+1;
         END WHILE;
       END//
DELIMITER ;
 
CALL purge_table('test','thist','created',90,10000);
