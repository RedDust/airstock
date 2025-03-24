
def GetSwitchData():

    sqlSelectSwitch  = " SELECT * FROM kt_realty_swtich "
    sqlSelectSwitch += " WHERE switch_type LIKE '10%' "
    sqlSelectSwitch += " AND state = '0' "
    sqlSelectSwitch += " ORDER BY process_sequence"
    sqlSelectSwitch += " , start_time ASC "

    return sqlSelectSwitch