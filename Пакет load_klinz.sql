/*<TOAD_FILE_CHUNK>*/
CREATE OR REPLACE PACKAGE KLINZ_LOAD AS
     TYPE t_klinz_tr_rc IS REF CURSOR RETURN KLINZ_TRANSIT%ROWTYPE;
     v_load_regim PLS_INTEGER; -- режим загрузки (0 -  устаревший, 1 - актуальный) 
     v_fetch_row_limit constant pls_integer := 100;
     v_fetch_sk_limit constant pls_integer := 10;
     
     PROCEDURE LOADER;
     PROCEDURE LOAD_KLINZ_TRANSIT;
     PROCEDURE LOAD_KLINZ;
     PROCEDURE LOAD_KLINZ_SK(SK IN SPSKV.SK_1%TYPE, SK_buf IN varchar2, l_cursor IN t_klinz_tr_rc);
          
     -- GET_CHANGES, PROCESS_CHANGES должны быть приватными, но пришлось опубликовать, иначе в SQL нельзя использовать
     FUNCTION GET_CHANGES(SK IN SPSKV.SK_1%TYPE DEFAULT null) RETURN t_klinz_tr_rc;
     FUNCTION GET_HIERARCH_CHANGES(SK IN SPSKV.SK_1%TYPE DEFAULT null) RETURN sys_refcursor;
     FUNCTION PROCESS_CHANGES(l_cursor IN t_klinz_tr_rc) RETURN t_corrdb_tab PIPELINED PARALLEL_ENABLE (PARTITION l_cursor BY ANY);
     
      
     PROCEDURE WRITE_MESS(NAME_PROC IN MESS.NAME_PROC%TYPE, DATE_WORK IN MESS.DATE_WORK%TYPE, 
                                            DESC_TASK IN MESS.DESC_TASK%TYPE, TYPE_MESS IN MESS.TYPE_MESS%TYPE, 
                                            MESS_TEXT IN MESS.MESS_TEXT%TYPE, ORDER_BY IN MESS.ORDER_BY%TYPE DEFAULT 99) ;
     PROCEDURE SET_LOAD_REGIM(load_regim IN pls_integer);
         
END;
/

/*<TOAD_FILE_CHUNK>*/
CREATE OR REPLACE PACKAGE BODY KLINZ_LOAD AS

    SUBTYPE  t_klinz_tr_rec IS KLINZ_TRANSIT%ROWTYPE;
    cnt NUMBER := 0;
    nameProcedure VARCHAR2(20) := 'KLINZ_LOAD_KAE';
     PROCEDURE SET_LOAD_REGIM(load_regim IN pls_integer)
     AS  
     BEGIN
           v_load_regim := load_regim;
    END;       
    
    PROCEDURE WRITE_MESS(NAME_PROC IN MESS.NAME_PROC%TYPE, DATE_WORK IN MESS.DATE_WORK%TYPE, 
                                            DESC_TASK IN MESS.DESC_TASK%TYPE, TYPE_MESS IN MESS.TYPE_MESS%TYPE, 
                                            MESS_TEXT IN MESS.MESS_TEXT%TYPE, ORDER_BY IN MESS.ORDER_BY%TYPE DEFAULT 99) 
     AS
     PRAGMA AUTONOMOUS_TRANSACTION;
     BIG_STRING_VALUE EXCEPTION;
     PRAGMA EXCEPTION_INIT(BIG_STRING_VALUE, -12899);
     error_string MESS.MESS_TEXT%TYPE;
     
     BEGIN
         cnt := cnt + 1;
        INSERT INTO MESS (NAME_PROC,DATE_WORK,DESC_TASK,TYPE_MESS,MESS_TEXT,ORDER_BY)
                     VALUES (NAME_PROC,DATE_WORK,DESC_TASK,TYPE_MESS,MESS_TEXT,cnt);
	    COMMIT;
        EXCEPTION 
        WHEN VALUE_ERROR OR BIG_STRING_VALUE THEN
                error_string := 'VALUE_ERROR on WRITE_MESS writing '||NAME_PROC;
                INSERT INTO MESS (NAME_PROC,DATE_WORK,DESC_TASK,TYPE_MESS,MESS_TEXT,ORDER_BY)
                         VALUES (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), null, 'E', error_string, cnt);
            COMMIT;
     END;
    
    PROCEDURE LOAD_KLINZ_TRANSIT
    AS
    BEGIN
    
       WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), 'START MERGE INTO KLINZ_TRANSIT', 'I', 'Start merging');
       MERGE INTO KLINZ_TRANSIT  KT
          USING (
               SELECT GEO.well SK_1,
                         GEO.BOTTOM_DEPTH GL_1,
                         GEO.ANGLE UG_1,
                         GEO.AZIMUTH AZ_1,
                         GEO.depth_abS ZZ_1,
                         GEO.DX XX_1,
                         GEO.DY YY_1,
                         -- обнуляет GEO.OFFSET для всех записей  размерность которых больше чем у колонки (5.1), 
                         --из ГЕОБД приходят не корректные данные иногда 
                         (CASE WHEN floor(GEO.OFFSET) > 9999 THEN 0 ELSE GEO.OFFSET END) SM_1,
                         GEO.DEPTH_PREV GLV_1,
                         GEO.BOREHOLE ZB2_1,
                        S.MS_1 MS_1,
                        S.S1_1 S1_1  
                    FROM  SPSKV S
                           inner join 
                            (SELECT wi.SOURCE_UWI well,
                              wi.Source_numbore borehole,
                              ptS.md bottom_depth,
                              ptS.tvd depth_abS,
                              ptS.deviatiON_angle angle,
                              nvl(ptS.azimuth,0) azimuth,
                              ptS.dx dx,
                              ptS.dy dy,
                              ROUND (SQRT (ptS.dx * ptS.dx + ptS.dy * ptS.dy),1) offSet,
                              NVL (LEAD (ptS.md)  OVER (PARTITION BY wi.uwi, wi.Source_numbore  ORDER BY ptS.md DESC), 0) depth_prev
                         FROM well_identificatiON@FINDER.WORLD wi
                         inner join well_dir_Srvy_hdr@FINDER.WORLD hdr on hdr.uwi = wi.uwi
                         inner join well_dir_Srvy_ptS@FINDER.WORLD ptS on ptS.uwi = wi.uwi and hdr.dir_srvy_id = pts.dir_srvy_id
                         WHERE hdr.preferred_flag = 'Y'
                              AND wi.Source <> 21
                              AND wi.Start_date <= SYSDATE
                              AND wi.end_date > SYSDATE) GEO  on GEO.well = S.sk_1) KT_NEW
         ON (KT.SK_1 = KT_NEW.SK_1 and KT.GL_1 = KT_NEW.GL_1 and KT.ZB2_1 = KT_NEW.ZB2_1)
         WHEN MATCHED THEN UPDATE SET KT.UG_1 = KT_NEW.UG_1, KT.AZ_1 = KT_NEW.AZ_1,
                                                         KT.ZZ_1 = KT_NEW.ZZ_1, KT.XX_1 = KT_NEW.XX_1,
                                                         KT.YY_1 = KT_NEW.YY_1, KT.SM_1 = KT_NEW.SM_1,
                                                         KT.GLV_1 = KT_NEW.GLV_1, KT.MS_1 = KT_NEW.MS_1,
                                                         KT.S1_1 = KT_NEW.S1_1 
         WHEN NOT MATCHED THEN INSERT (SK_1, GL_1, UG_1, AZ_1, ZZ_1, XX_1, YY_1, SM_1, GLV_1, ZB2_1, MS_1, S1_1)                        
            VALUES (KT_NEW.SK_1, KT_NEW.GL_1, KT_NEW.UG_1, KT_NEW.AZ_1, KT_NEW.ZZ_1, KT_NEW.XX_1,
                         KT_NEW.YY_1, KT_NEW.SM_1, KT_NEW.GLV_1, KT_NEW.ZB2_1, KT_NEW.MS_1, KT_NEW.S1_1)        
         LOG ERRORS INTO ERR$_KLINZ_TRANSIT (to_char(current_date)) REJECT LIMIT UNLIMITED;       
     
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), 'MERGE INTO KLINZ_TRANSIT',
                               'I', to_char(SQL%ROWCOUNT)||' rows were merged by procedure');

         COMMIT;
     END;
     
     FUNCTION GET_CHANGES(SK IN SPSKV.SK_1%TYPE DEFAULT null) RETURN t_klinz_tr_rc
     IS
         l_cursor t_klinz_tr_rc;
     BEGIN
   
         OPEN l_cursor FOR 
             SELECT * FROM KLINZ_TRANSIT KT
                 WHERE KT.SK_1 = (CASE WHEN SK IS NULL THEN KT.SK_1 ELSE SK END) 
                      AND NOT EXISTS
                                      (SELECT KL.SK_1
                                       FROM KLINZ KL
                                       WHERE KL.SK_1 = KT.SK_1
                                       AND KL.ZB2_1 = KT.ZB2_1
                                       AND KL.GL_1 = KT.GL_1);
        RETURN l_cursor;     
     END;
     FUNCTION GET_HIERARCH_CHANGES(SK IN SPSKV.SK_1%TYPE DEFAULT null) RETURN sys_refcursor
     IS
         l_cursor sys_refcursor;
     BEGIN
   
         OPEN l_cursor FOR Select distinct KT.SK_1,  KT.MS_1, KT.S1_1
                                             ,CURSOR(Select * 
                                                              From KLINZ_TRANSIT KT_IN
                                                                Where KT_IN.SK_1 = KT.SK_1
                                                                    AND NOT EXISTS
                                                                              (SELECT KL.SK_1
                                                                               FROM KLINZ KL
                                                                               WHERE KL.SK_1 = KT_IN.SK_1
                                                                                    AND KL.ZB2_1 = KT_IN.ZB2_1
                                                                                    AND KL.GL_1 = KT_IN.GL_1))
                                                FROM KLINZ_TRANSIT KT
                                                WHERE KT.SK_1 = (CASE WHEN SK IS NULL THEN KT.SK_1 ELSE SK END) 
                                                        AND NOT EXISTS
                                                                      (SELECT KL.SK_1
                                                                       FROM KLINZ KL
                                                                       WHERE KL.SK_1 = KT.SK_1
                                                                       AND KL.ZB2_1 = KT.ZB2_1
                                                                       AND KL.GL_1 = KT.GL_1);
      RETURN l_cursor;     
     END;
     
    FUNCTION PROCESS_CHANGES(l_cursor IN t_klinz_tr_rc) RETURN t_corrdb_tab 
    -- преобразуем строки таблицы klinz_transit в строки таблицы corrdb
                                                                                                                       
                           PIPELINED
                           PARALLEL_ENABLE (PARTITION l_cursor BY ANY)
    IS
          r_target_data t_corrdb_rec := t_corrdb_rec(null,null,null,null,null,null,null,null);
          --r_source_data t_klinz_tr_rec;
          type t_aa_tab is table of t_klinz_tr_rec index by pls_integer;
          v_tab t_aa_tab;
          v_computerName CORRDB.COMPUTER_NAME%TYPE;
          function make_target_row(r_target_data IN OUT t_corrdb_rec,
                                                    computerName IN CORRDB.COMPUTER_NAME%TYPE,
                                                    col_name IN CORRDB.NAME_COLUMN%TYPE,
                                                    col_value IN CORRDB.VALUE_COLUMN%TYPE) RETURN t_corrdb_rec
             IS
             BEGIN
                  r_target_data.name_column := col_name;
                  r_target_data.value_column := col_value;
                  r_target_data.computer_name := computerName;
                  RETURN r_target_data;
             END;
             
    BEGIN
        r_target_data.Kpl  := '  ';
        r_target_data.name_table  := 'KLINZ';
        r_target_data.priority := 99;
        r_target_data.priority_field := '0';
        r_target_data.name_user := nameProcedure;
       LOOP
          FETCH l_cursor BULK COLLECT INTO v_tab LIMIT v_fetch_row_limit;
          EXIT WHEN v_tab.COUNT = 0;
          FOR ind IN 1..v_tab.COUNT LOOP
              -- превращаем одну строку курсора  в  столько строк сколько столбцов  в таблице, в corrdb они будут в отдельных строках
              
               -- computer_name должен быть одинаковый у  строк полученных из одной строки курсора и представлять собой ключ таблицы klinz
               -- v_computerName := '$'||to_char(v_tab(ind).sk_1)||'$'||to_char(v_tab(ind).gl_1)||'$'||to_char(v_tab(ind).zb2_1);
               -- номер скважины сохраняем в специальном формате - формате буфера OIS
               v_computerName := '$'||to_char(v_tab(ind).ms_1)||' '||trim(to_char(v_tab(ind).s1_1))||'$'||to_char(v_tab(ind).gl_1)||'$'||to_char(v_tab(ind).zb2_1);
               
               -- выборка столбцов из  user_tab_cols для таблицы klinz для прохода в цикле неоптимальна - обращение к словарю данных
               -- поэтому перечисляем вручную.
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'SK', v_tab(ind).SK_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'MS', v_tab(ind).MS_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'S1', v_tab(ind).S1_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'GL', v_tab(ind).GL_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'UG', v_tab(ind).UG_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'AZ', v_tab(ind).AZ_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'ZZ', v_tab(ind).ZZ_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'XX', v_tab(ind).XX_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'YY', v_tab(ind).YY_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'SM', v_tab(ind).SM_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'GLV', v_tab(ind).GLV_1));
               PIPE ROW (make_target_row(r_target_data, v_computerName, 'ZB2', v_tab(ind).ZB2_1));
           END LOOP;
        END LOOP;
        CLOSE l_cursor;
        RETURN;
    END;
    FUNCTION getSK(computer_name IN CORRDB.COMPUTER_NAME%TYPE) RETURN VARCHAR2
        IS
    BEGIN
         -- SK_buf берем как первую подстроку между знаками $
        RETURN substr(computer_name,2,instr(computer_name,'$',2,1)-2); 
        
    END;
     
    PROCEDURE LOAD_KLINZ_SK(SK IN SPSKV.SK_1%TYPE, SK_buf IN varchar2, l_cursor IN t_klinz_tr_rc)
    AS
        
     BEGIN 
         -- comment:                                     
         --v_computerName := '$'||to_char(SK)||'$'||to_char(v_tab(ind).gl_1)||'$'||to_char(v_tab(ind).zb2_1);
         /*DELETE CORRDB WHERE name_user = nameProcedure;   можно раскомментировать если будет потребность*/   
         
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' Start INSERT INTO CORRDB',
                               'I', ' Start load CORRDB SK_1 =  '||SK);
         INSERT INTO CORRDB (Kpl,name_TABLE,name_column,value_column,priority,priority_field,name_uSer,computer_name)
             SELECT kpl,name_TABLE,name_column,value_column,priority,priority_field,name_uSer,computer_name
                 FROM TABLE (PROCESS_CHANGES(l_cursor))
         LOG ERRORS INTO ERR$_CORRDB(to_char(current_date)) REJECT LIMIT UNLIMITED;     
         
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' Finish INSERT INTO CORRDB',
                               'I', to_char(SQL%ROWCOUNT)||' rows were inserted by proc LOAD_KLINZ_SK, SK = '||SK);
        COMMIT;
        /* теперь надо вызывать процедуру  для работу с буфером OIS Production, которая все строки из CORRDB
        запишет в базу в таблицу  klinz  - в цикле, медленно и печально*/          
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' CALL MAINCORRECTION',
                               'I', to_char(SQL%ROWCOUNT)||' START LOOP CALL MAINCORRECTION , SK = '||SK);                      
         FOR curRow IN (Select distinct computer_name as computer
                                From CORRDB
                                   WHERE  name_user = nameProcedure)                  
        LOOP                   
               CORRECTDB.MainCorrectiON(curRow.computer, SK_buf,null,null);        
        END LOOP;
        WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' CALL MAINCORRECTION',
                               'I', to_char(SQL%ROWCOUNT)||' Finish LOOP CALL MAINCORRECTION, SK = '||SK);  
                          
        EXCEPTION 
        WHEN OTHERS THEN
        WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' Error',
                               'E', ' Error while loading CORRDB SK_1 =  '||SK_buf);     
           DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_STACK); 
         RAISE;
    END;
    
    PROCEDURE LOAD_KLINZ
    AS
                                   
     BEGIN  
         --v_computerName := '$'||to_char(SK)||'$'||to_char(v_tab(ind).gl_1)||'$'||to_char(v_tab(ind).zb2_1);
         
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' Start INSERT INTO CORRDB',
                               'I', ' Start load CORRDB');
         INSERT INTO CORRDB (Kpl,name_TABLE,name_column,value_column,priority,priority_field,name_uSer,computer_name)
             SELECT kpl,name_TABLE,name_column,value_column,priority,priority_field,name_uSer,computer_name
                 FROM TABLE (PROCESS_CHANGES(GET_CHANGES))
         LOG ERRORS INTO ERR$_CORRDB(to_char(current_date)) REJECT LIMIT UNLIMITED;     
         
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' Finish INSERT INTO CORRDB',
                               'I', to_char(SQL%ROWCOUNT)||' rows were inserted by proc LOAD_KLINZ');
        COMMIT;
        /* теперь надо вызывать процедуру  для работу с буфером OIS Production, которая все строки из CORRDB
        запишет в базу в таблицу  klinz  - в цикле, медленно и печально*/          
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' CALL MAINCORRECTION',
                               'I', ' START LOOP CALL MAINCORRECTION');                      
         FOR curRow IN (Select distinct computer_name as computer
                                From CORRDB
                                   WHERE  name_user = nameProcedure)                  
        LOOP   
               CORRECTDB.MainCorrectiON(curRow.computer, getSK(curRow.computer),null,null);  
        END LOOP;
        WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' CALL MAINCORRECTION',
                               'I', ' Finish LOOP CALL MAINCORRECTION');  
                          
        EXCEPTION 
        WHEN OTHERS THEN
        WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' Error',
                               'E', ' Error while loading CORRDB by PROCEDURE LOAD_KLINZ');     
           DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_STACK); --
         RAISE;
     END;
     
    PROCEDURE LOADER
    AS
         
         l_cursor t_klinz_tr_rc;
         gl_cursor sys_refcursor;
         v_S1  varchar2(15);  
         v_MS varchar2(15);  
         v_SK  varchar2(15);
         v_SK_buf  varchar2(15);
         /* - такой вложенный тип не разрешен в  текущем релизе оракл для записей, объектов, таблиц
           type  t_aa_rec is record ( SK klinz_transit.sk_1%TYPE,
                                              MS klinz_transit.ms_1%TYPE,
                                              S1 klinz_transit.s1_1%TYPE,
                                              l_cursor t_klinz_tr_rc);  
           type  t_aa_tab is table of t_aa_rec index by pls_integer;
           v_tab  t_aa_tab; */ 
    BEGIN
          /* сначала загрузим изменения в таблицу  KLINZ_TRANSIT (здесь будут все изменения)
          но в klinz будем загружать только те сочетания SK_1, GL_1, ZB2_1, которых нет 
        (так как процедура main_correction не предполагает Update и Delete, только Insert)
        
               
        */
        /* очищаем corrdb  в начале загрузки */
        DELETE FROM MESS WHERE NAME_PROC=RPAD(nameProcedure, 20);  
        DELETE CORRDB WHERE name_user = nameProcedure;
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' DELETE FROM CORRDB',
                               'I', to_char(SQL%ROWCOUNT)||' rows were deleted by procedure LOADER');
                               
         LOAD_KLINZ_TRANSIT;
         
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' Start load to KLINZ',
                               'I', ' Start load to KLINZ');
         
         IF  v_load_regim = 1  THEN -- текущая версия загрузки (грузим все данные вместе в corrdb)
              LOAD_KLINZ;
         ELSIF v_load_regim = 0  THEN  -- устаревшая версия загрузки ( в corrdb грузим по одной скважине)
            gl_cursor := GET_HIERARCH_CHANGES(); --получаем  иерархическую таблицу изменений Скважина - Изменения по скважине (отсутствующие строки в klinz с уникальным ключом)
                                                                         -- и грузим порциями по каждой скважине
            LOOP
                FETCH gl_cursor INTO v_SK, v_MS, v_S1, l_cursor;
                EXIT WHEN gl_cursor%NOTFOUND;
                
                 v_SK_buf := v_MS||' '||v_S1;
                 LOAD_KLINZ_SK(v_SK, v_SK_buf, l_cursor); -- процедура загрузки  в corrdb вызывается для каждой скважины
                
            END LOOP;
             CLOSE gl_cursor;
         END IF;                      
         
         WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' Finish load to KLINZ',
                           'I', ' Finish load to KLINZ'); 
                           
         EXCEPTION
         WHEN OTHERS THEN                   
            WRITE_MESS (nameProcedure, to_char(SYSDATE,'dd.mm.yyyy hh24:mi:ss'), ' Error',
                               'E', ' Error in procedure LOADER ');     
           DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_STACK); 
           RAISE;
    END;
    
  BEGIN -- блок инициализации
        --EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
        -- устанавливаем режим загрузки (0 - первичный, 1 - повторный) используется разный режим загрузки, 
        --так как при первичной загрузке данных очень много. 1 - по умолчанию
        SET_LOAD_REGIM(1);
  END;
    /

