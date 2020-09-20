/*
It is only an example of query that manage functions as table

Author: Tonny Costa Cordeiro
*/

CREATE OR REPLACE TYPE WORKSHIFT_DATA_TYPE
IS OBJECT (
    workshift VARCHAR2(5),
    workshiftDate DATE,
    workshiftDateTS VARCHAR2(8),
    workshiftStart VARCHAR2(16),
    workshiftEnd VARCHAR2(16)
)
/

CREATE OR REPLACE TYPE WORKSHIFT_TABLE_TYPE
AS TABLE OF WORKSHIFT_DATA_TYPE
/

CREATE OR REPLACE FUNCTION FN_GET_WORKSHIFT_TABLE (DATE_STAMP_START VARCHAR2, DATE_STAMP_END VARCHAR2)
RETURN WORKSHIFT_TABLE_TYPE PIPELINED
IS
	DATE_FORMAT_VALUE VARCHAR2(20) := '';
	DATE_TIME_FORMAT_VALUE VARCHAR2(20) := '';
	WORK_SHIFT_START VARCHAR2(5) := '07-19';
	WORK_SHIFT_END VARCHAR2(5) := '19-07';

BEGIN

FOR RECORD_OUTPUT IN (

	select '07-19' workshift, dt.COLUMN_VALUE workshiftDate, to_char(dt.COLUMN_VALUE,DATE_FORMAT_VALUE) workshiftDateTS, 
								to_char(to_date(substr(fn_datetime_to_string(dt.COLUMN_VALUE), 1, 8),
                                                  'YYYYMMDD') +
                                          (cast(substr(WORK_SHIFT_START, 1, 2) as int) -
                                           (case when exists
                                                 (select summer_period_tzoffset x
                                                    from reporting.brazil_timezone
                                                    where state = 'SP'
                                                      and dt.COLUMN_VALUE  between SUMMER_PERIOD_LOCAL_START and SUMMER_PERIOD_LOCAL_END)
                                                 then -2 else -3 end)) / 24,
                                          DATE_TIME_FORMAT_VALUE) || 'UT' workshiftStart,

                                to_char(to_date(substr(fn_datetime_to_string(dt.COLUMN_VALUE), 1, 8),
                                                  DATE_FORMAT_VALUE) +
                                          (cast(substr(WORK_SHIFT_START, 4, 2) as int) -
                                           (case when exists
                                                 (select summer_period_tzoffset x
                                                    from reporting.brazil_timezone
                                                    where state = 'SP'
                                                      and dt.COLUMN_VALUE  between SUMMER_PERIOD_LOCAL_START and SUMMER_PERIOD_LOCAL_END)
                                                 then -2 else -3 end)) / 24 + (case when (cast(substr('07-19',4,2) as int) < cast(substr('07-19',1,2) as int)) then 1 else 0 end),
                                          DATE_TIME_FORMAT_VALUE) || 'UT' workshiftEnd
                             from Dual cross join table(FN_GET_DATES(DATE_STAMP_START, DATE_STAMP_END)) dt
            union
                select '19-07' workshift, dt.COLUMN_VALUE workshiftDate, to_char(dt.COLUMN_VALUE,'YYYYMMDD') workshiftDateTS, 
								to_char(to_date(substr(fn_datetime_to_string(dt.COLUMN_VALUE), 1, 8),
                                                  DATE_FORMAT_VALUE) +
                                          (cast(substr(WORK_SHIFT_END, 1, 2) as int) -
                                           (case when exists
                                                 (select summer_period_tzoffset x
                                                    from reporting.brazil_timezone
                                                    where state = 'SP'
                                                      and dt.COLUMN_VALUE  between SUMMER_PERIOD_LOCAL_START and SUMMER_PERIOD_LOCAL_END)
                                                 then -2 else -3 end)) / 24,
                                          DATE_TIME_FORMAT_VALUE) || 'UT' workshiftStart,

								to_char(to_date(substr(fn_datetime_to_string(dt.COLUMN_VALUE), 1, 8),
											  DATE_FORMAT_VALUE) +
                                          (cast(substr(WORK_SHIFT_END, 4, 2) as int) -
                                           (case when exists
                                                 (select summer_period_tzoffset x
                                                    from reporting.brazil_timezone
                                                    where state = 'SP'
                                                      and dt.COLUMN_VALUE  between SUMMER_PERIOD_LOCAL_START and SUMMER_PERIOD_LOCAL_END)
                                                 then -2 else -3 end)) / 24 + (case when (cast(substr('19-07',4,2) as int) < cast(substr('19-07',1,2) as int)) then 1 else 0 end),
                                          DATE_TIME_FORMAT_VALUE) || 'UT' workshiftEnd
                             from Dual cross join table(FN_GET_DATES(DATE_STAMP_START, DATE_STAMP_END)) dt
    )
    LOOP
        PIPE ROW (WORKSHIFT_DATA_TYPE(RECORD_OUTPUT.workshift, RECORD_OUTPUT.workshiftDate,RECORD_OUTPUT.workshiftDateTS,RECORD_OUTPUT.workshiftStart,RECORD_OUTPUT.workshiftEnd));
    END LOOP;

END;



--Example:
select 
	wsch.unit_id,
	wsch.category,
	wsch.work_shift,
	wsch.work_schedule_date,
	(select count(distinct event_id)
	   from vehicle_history vh
	  where vh.creation_date 
		between wsft.workshiftStart and ws.workshiftEnd
		and vh.vehicle_id = wsch.unit_id
		and vh.unit_status = 'DP') Qt_DP,
	(select count(distinct event_id)
	   from vehicle_history vh
	  where vh.creation_date 
		between wsft.workshiftStart and ws.workshiftEnd
		and vh.vehicle_id = wsch.unit_id
		and vh.unit_status = 'TH') Qt_TH,
	(select min(vh.creation_ts) min_DM_dts
	   from vehicle_history vh
	  where vh.creation_date 
		between wsft.workshiftStart and ws.workshiftEnd
		and vh.vehicle_id = wsch.unit_id
		and vh.unit_status = 'DM') Qt_DM
from work_schedule wsch
inner join 
	(select dt.WORKSHIFT, dt.WORKSHIFTDATE, dt.WORKSHIFTDATETS, dt.WORKSHIFTSTART, dt.WORKSHIFTEND
		from table(FN_GET_WORKSHIFT_TABLE(date_stamp_start => substr(fn_datetime_to_string({?WorkScheduleStartDate}),0,8),
                                     date_stamp_end => substr(fn_datetime_to_string({?WorkScheduleEndDate}),0,8))) dt) wsft
	on wsch.work_shift = wsft.WORKSHIFT and wsch.work_schedule_date = wsft.WORKSHIFTDATETS
	where wsch.work_schedule_date between 
		substr(fn_datetime_to_string({?WorkScheduleStartDate}),0,8)
		and substr(fn_datetime_to_string({?WorkScheduleEndDate}),0,8)
					 
