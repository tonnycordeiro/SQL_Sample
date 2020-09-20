/*
It is a complex query that cross quantity informations about priority of emergency events and status of vehicles dispatched to attend them by each 10 minutes of interval on day.
Besides the usual SQL structures, it was built also with "analytic functions", "hints", "pivoting" and "unpivoting".

Author: Tonny Costa Cordeiro
*/

select 	count_priority.*,
		count_vehicle_status.DISPONIVEIS, count_vehicle_status.DESPACHADOS, count_vehicle_status.EM_ROTA, 
		count_vehicle_status.SAIDA_LOCAL,count_vehicle_status.CHEGADA_LOCAL,count_vehicle_status.EM_TRANSICAO
	from (select decode(priority,
                      1,
                      'ALTISSIMA',
                      2,
                      'ALTA',
                      3,
                      'MEDIA',
                      4,
                      'BAIXA',
                      5,
                      'BAIXISSIMA',
                      'SEM PRIORIDADE') status,
               event_id item,
               hour_time,
               minute_time,
               reporting.sgph_string_to_datetime(dts) date_time
			from 
				(select evth.event_id,
                       evth.priority,
                       evth.cdts start_cdts,
                       (case when evth.udts is null then
                          to_char(
							to_date(substr(evth.cdts, 1, 14),'YYYYMMDDHH24MISS') - (1 / 24 / 60 / 60),
                            'YYYYMMDDHH24MISS'
						  ) || 'UT'
                         else
                          evth.udts
                       end) end_cdts,
                       d.start_date,
                       d.end_date,
                       evth.rev_num
                  from vw_emergency_event_history evth
					inner join vw_emergency_event evt
						on evt.event_id = evth.event_id
					--the start_date and end_date have not been calculated by function to not prejudice the query's performance:
					inner join (select to_char(
										to_date(fn_date_to_string({?WorkScheduleDate}),'YYYYMMDDHH24MISS') +
											(-cast(to_char(FROM_TZ(cast(to_date((fn_date_to_string({?WorkScheduleDate})),
																			   'YYYYMMDDHH24MISS') as
																	   TIMESTAMP),
																  'America/Sao_Paulo'),
														  'TZH') as int)) / 24,
                                        'YYYYMMDDHH24MI') || '00UT' start_date,
									   to_char(
										to_date(fn_date_to_string({?WorkScheduleDate}) || '2359','YYYYMMDDHH24MISS') +
											   (-cast(to_char(FROM_TZ(cast(to_date(fn_date_to_string({?WorkScheduleDate}),
																				   'YYYYMMDDHH24MISS') as
																		   TIMESTAMP),
																	  'America/Sao_Paulo'),
															  'TZH') as int)) / 24,
											   'YYYYMMDDHH24MI') || '59UT' end_date
                              from Dual) d
						on evt.ad_ts <= d.end_date and evt.udts >= d.start_date
							and evth.status_code = 7
			) aep
			inner join 
				(select hour_time,
						minute_time,
						to_char(
							to_date(fn_date_to_string({?WorkScheduleDate}),'YYYYMMDD') +
							   (- (cast(to_char(FROM_TZ(cast(to_date(fn_date_to_string({?WorkScheduleDate}),
																	'YYYYMMDDHH') as
															TIMESTAMP),
													   'America/Sao_Paulo'),
											   'TZH') as int) * 60) +
								 (hour_time * 60) + minute_time) / 24 / 60,
							   'YYYYMMDDHH24MISS') || 'UT' dts
                      from (select hour_time, minute_time
                              from ((select 0  h0,
                                            1  h1,
                                            2  h2,
                                            3  h3,
                                            4  h4,
                                            5  h5,
                                            6  h6,
                                            7  h7,
                                            8  h8,
                                            9  h9,
                                            10 h10,
                                            11 h11,
                                            12 h12,
                                            13 h13,
                                            14 h14,
                                            15 h15,
                                            16 h16,
                                            17 h17,
                                            18 h18,
                                            19 h19,
                                            20 h20,
                                            21 h21,
                                            22 h22,
                                            23 h23
                                       from Dual)
                                    unpivot(hour_time for value_type in (h0,
                                                           h1,
                                                           h2,
                                                           h3,
                                                           h4,
                                                           h5,
                                                           h6,
                                                           h7,
                                                           h8,
                                                           h9,
                                                           h10,
                                                           h11,
                                                           h12,
                                                           h13,
                                                           h14,
                                                           h15,
                                                           h16,
                                                           h17,
                                                           h18,
                                                           h19,
                                                           h20,
                                                           h21,
                                                           h22,
                                                           h23)))
                             cross join (select minute_time
                                          from ((select 0  m0,
                                                        10 m10,
                                                        20 m20,
                                                        30 m30,
                                                        40 m40,
                                                        50 m50
                                                   from Dual)
                                                unpivot(minute_time for
                                                        value_type in (m0,
                                                                       m10,
                                                                       m20,
                                                                       m30,
                                                                       m40,
                                                                       m50))))
                             where fn_date_to_string({?WorkScheduleDate}) || lpad(hour_time, 2, '0') || lpad(minute_time, 2, '0') || '00UT' 
								between (fn_date_to_string({?WorkScheduleDate}) || '000000UT') and
										(fn_date_to_string({?WorkScheduleDate}) || '235959UT')
						)
					)
            on dts between aep.start_cdts and aep.end_cdts
         group by priority, hour_time, minute_time, event_id, dts
         order by hour_time, minute_time, event_id) pivot(count(distinct item) for status in(
																				'BAIXISSIMA' BAIXISSIMA,
                                                                                'BAIXA' BAIXA,
                                                                                'MEDIA' MEDIA,
                                                                                'ALTA' ALTA,
                                                                                'ALTISSIMA' ALTISSIMA)) count_priority
	inner join (select *
               from (select vehicle_status,
                            hour_time,
                            minute_time,
                            vehicle_id,
                            reporting.sgph_string_to_datetime(dts) date_time
                       from (select vhst1.vehicle_id,
                                    decode(vhst1.vehicle_status,
                                           'DM',
                                           'DISPONÍVEIS',
                                           'DP',
                                           'DESPACHADOS',
                                           'ER',
                                           'EM ROTA',
                                           'SL',
                                           'SAÍDA LOCAL',
                                           'CH',
                                           'CHEGADA LOCAL',
                                           'TH',
                                           'TRANSIÇÃO',
                                           'FS',
                                           'FORA DE SERVIÇO',
                                           '') vehicle_status,
                                    vhst1.start_cdts,
                                    decode(vhst1.end_position,
                                           1,
                                           vhst1.end_date,
                                           to_char(
												to_date(substr(vhst1.end_cdts, 1, 14),'YYYYMMDDHH24MISS') - (1 / 24 / 60 / 60),
												'YYYYMMDDHH24MISS'
											  ) || 'UT') end_cdts,
                                    vhst1.start_position,
                                    vhst1.end_position
                               from (select distinct vhst.vehicle_id,
                                                     vhst.vehicle_status,
                                                     vhst.cdts start_cdts,
                                                     last_value(vhst.cdts) ignore nulls over(partition by vhst.vehicle_id order by vhst.cdts, vhst.agency_event_rev_num ROWS between current row and 1 following) end_cdts,
                                                     rank() over(partition by vhst.vehicle_id order by vhst.cdts, vhst.agency_event_rev_num) start_position,
                                                     rank() over(partition by vhst.vehicle_id order by vhst.cdts desc, vhst.agency_event_rev_num desc) end_position,
                                                     vhst.start_date,
                                                     vhst.end_date
                                       FROM (select /*+ index (vh idx_vehicle_hist_cdts_status) */ vh.vehicle_id,
                                                    decode(vh.vehicle_status,
                                                           'DI',
                                                           'DM',
                                                           'AK',
                                                           'SL',
                                                           vh.vehicle_status) vehicle_status,
                                                    vh.cdts,
                                                    d.start_date,
                                                    d.end_date,
                                                    NVL(vh.agency_event_rev_num, 1000) agency_event_rev_num
                                               from vehicle_history vh
											  --the start_date and end_date have not been calculated by function to not prejudice the query's performance:
                                              inner join (select to_char(
																	to_date(fn_date_to_string({?WorkScheduleDate}),'YYYYMMDDHH24MISS') +
																		(-cast(to_char(FROM_TZ(cast(to_date((fn_date_to_string({?WorkScheduleDate})),
																										   'YYYYMMDDHH24MISS') as
																								   TIMESTAMP),
																							  'America/Sao_Paulo'),
																					  'TZH') as int)) / 24,
																	'YYYYMMDDHH24MI') || '00UT' start_date,
																   to_char(
																	to_date(fn_date_to_string({?WorkScheduleDate}) || '2359','YYYYMMDDHH24MISS') +
																		   (-cast(to_char(FROM_TZ(cast(to_date(fn_date_to_string({?WorkScheduleDate}),
																											   'YYYYMMDDHH24MISS') as
																									   TIMESTAMP),
																								  'America/Sao_Paulo'),
																						  'TZH') as int)) / 24,
																		   'YYYYMMDDHH24MI') || '59UT' end_date
                                                           from Dual) d
                                                 on vh.cdts between
                                                    d.start_date and d.end_date
                                              where vh.vehicle_status in
                                                    (select col2_value status_acronym
                                                       from cfg_parameters_table_rows
                                                      where set_name = 'WorkSchedule'
                                                        and table_name = 'VehicleStatus'
                                                        and cfg_name = 'all')
                                             UNION (select distinct uhf.vehicle_id vehicle_id,
                                                                   first_value(decode(uhf.vehicle_status,
                                                                                      'DI',
                                                                                      'DM',
																					  'AK',
																					  'SL',
                                                                                      uhf.vehicle_status)) over(partition by uhf.vehicle_id order by uhf.cdts desc, NVL(uhf.agency_event_rev_num, 0) desc) vehicle_status,
                                                                   d.start_date cdts,
                                                                   d.start_date,
                                                                   d.end_date,
                                                                   0 agency_event_rev_num
                                                     from vehicle_history uhf
													--the start_date and end_date have not been calculated by function to not prejudice the query's performance:
                                                    inner join (select to_char(
																	to_date(fn_date_to_string({?WorkScheduleDate}),'YYYYMMDDHH24MISS') +
																		(-cast(to_char(FROM_TZ(cast(to_date((fn_date_to_string({?WorkScheduleDate})),
																										   'YYYYMMDDHH24MISS') as
																								   TIMESTAMP),
																							  'America/Sao_Paulo'),
																					  'TZH') as int)) / 24,
																	'YYYYMMDDHH24MI') || '00UT' start_date,
																   to_char(
																	to_date(fn_date_to_string({?WorkScheduleDate}) || '2359','YYYYMMDDHH24MISS') +
																		   (-cast(to_char(FROM_TZ(cast(to_date(fn_date_to_string({?WorkScheduleDate}),
																											   'YYYYMMDDHH24MISS') as
																									   TIMESTAMP),
																								  'America/Sao_Paulo'),
																						  'TZH') as int)) / 24,
																		   'YYYYMMDDHH24MI') || '59UT' end_date,
                                                                    to_char(
																		(to_date(fn_date_to_string({?WorkScheduleDate}),'YYYYMMDDHH24MISS') - 3),
                                                                              'YYYYMMDDHH24MISS') time_ago
                                                                 from Dual) d
                                                       on uhf.cdts between
                                                          d.time_ago and
                                                          d.start_date
                                                    where uhf.vehicle_status in
                                                          (select col2_value status_acronym
                                                             from cfg_param_table_rows
                                                            where set_name = 'WorkSchedule'
                                                              and table_name = 'VehicleStatus'
                                                              and cfg_name = 'all'))) vhst) vhst1)
                      inner join (select hour_time,
                                        minute_time,
                                        to_char(
											to_date(fn_date_to_string({?WorkScheduleDate}),'YYYYMMDD') +
                                                (- (cast(to_char(FROM_TZ(cast(to_date(fn_date_to_string({?WorkScheduleDate}),
                                                                                     'YYYYMMDDHH') as
                                                                             TIMESTAMP),
                                                                        'America/Sao_Paulo'),
                                                                'TZH') as int) * 60) +
                                                  (hour_time * 60) + minute_time) / 24 / 60,
                                                'YYYYMMDDHH24MISS') || 'UT' dts
                                   from (select hour_time, minute_time
                                           from ((select 0  h0,
                                                         1  h1,
                                                         2  h2,
                                                         3  h3,
                                                         4  h4,
                                                         5  h5,
                                                         6  h6,
                                                         7  h7,
                                                         8  h8,
                                                         9  h9,
                                                         10 h10,
                                                         11 h11,
                                                         12 h12,
                                                         13 h13,
                                                         14 h14,
                                                         15 h15,
                                                         16 h16,
                                                         17 h17,
                                                         18 h18,
                                                         19 h19,
                                                         20 h20,
                                                         21 h21,
                                                         22 h22,
                                                         23 h23
                                                    from Dual)
                                                 unpivot(hour_time for
                                                         value_type in (h0,
                                                                        h1,
                                                                        h2,
                                                                        h3,
                                                                        h4,
                                                                        h5,
                                                                        h6,
                                                                        h7,
                                                                        h8,
                                                                        h9,
                                                                        h10,
                                                                        h11,
                                                                        h12,
                                                                        h13,
                                                                        h14,
                                                                        h15,
                                                                        h16,
                                                                        h17,
                                                                        h18,
                                                                        h19,
                                                                        h20,
                                                                        h21,
                                                                        h22,
                                                                        h23)))
                                          cross join (select minute_time
                                                       from ((select 0  m0,
                                                                     10 m10,
                                                                     20 m20,
                                                                     30 m30,
                                                                     40 m40,
                                                                     50 m50
                                                                from Dual)
                                                             unpivot(minute_time for
                                                                     value_type in (m0,
                                                                                    m10,
                                                                                    m20,
                                                                                    m30,
                                                                                    m40,
                                                                                    m50))))
                                          where (fn_date_to_string({?WorkScheduleDate}) || lpad(hour_time, 2, '0') || lpad(minute_time, 2, '0') || '00UT') 		between
													(fn_date_to_string({?WorkScheduleDate}) || '000000UT') and
													(fn_date_to_string({?WorkScheduleDate}) || '235959UT')
										)
								)
                         on dts between start_cdts and end_cdts
                      group by vehicle_status, hour_time, minute_time, vehicle_id, dts
                      order by hour_time, minute_time, vehicle_id) pivot(count(distinct vehicle_id) for vehicle_status in(
																								'DISPONÍVEIS' DISPONIVEIS,
                                                                                                 'DESPACHADOS' DESPACHADOS,
                                                                                                 'EM ROTA' EM_ROTA,
                                                                                                 'SAÍDA LOCAL' SAIDA_LOCAL,
                                                                                                 'CHEGADA LOCAL' CHEGADA_LOCAL,
                                                                                                 'TRANSIÇÃO' EM_TRANSICAO))) count_vehicle_status
    on (count_priority.hour_time = count_vehicle_status.hour_time and count_priority.minute_time = count_vehicle_status.minute_time 
		and count_priority.date_time = count_vehicle_status.date_time) 