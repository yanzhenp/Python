#!/bin/env bash
# This script is to analyze IC/RZ tables' constraints.
# Written by : Arvin
# Date: 6/21/2016
# Usage: ./AnalyzeConstraints.sh -table <table_name> -asset <asset_name>

# if number of arguments is not correct, then print HELP text.
# Modify history:
#       Arvin,2016-06-21: Initial
#       Arvin,2016-06-22: Add FK and UK constraint check, bug fixes and code improvement
#       Arvin,2016-06-23: Bland space may cause offset in resultset, fixed
#       Arvin,2016-06-24: Fixed bug when timestamp field may not be converted to string
#       Arvin,2016-06-28: Add a parameter to specify asset name.
#       Arvin,2016-06-29: Modify script to enable rz constraint check
#       Arvin,2016-06-30: Skip checking when PK/FK list is empty
#       Arvin,2016-07-06: Add self-reference check for FK
#       Arvin,2016-07-25: optimize code
#       Arvin,2016-07-26: fixed space issue in fk values
#       Arvin, 12/8/2016: Add 'key' and 'rfnc' option
#       Arvin,2016-09-14: Add null option
#	Arvin,2016-10-12: insert constraint values into Hive table

function Help()
{
cat << HELP
Command Usage
NAME
       AnalyzeConstraints - Analyze Constraints for IC table

SYNOPSIS
       AnalyzeConstraints - [OPTION]...

DESCRIPTION
       Analyze Constraints for specified IC table, print error values
           -table
                        specify table's name that needs to be analyzed
           -asset
                        specify asset name for target table

EXAMPLE
       [User@Host~]$ sh $sh/Analyzeconstraints.sh -table radar_rz.sm_atr_dmnsn -asset FI1 [-key 'f' [-rfnc 'addr']]

AUTHOR
       Written by Arvin.
HELP
return 0
}

if [ $# -lt 4 ]; then
        Help
        exit 1;
else
        while [ $# -gt 0 ]
        do
        case $1 in
        -table)
            TABLE_NAME=$2;
            shift;
            ;;
        -asset)
            ASSET_NAME=`echo $2 | tr [:lower:] [:upper:]`;
            shift;
            ;;
        -key)
	    KEY=`echo $2 | tr [:lower:] [:upper:]`;
	    shift;
	    ;;
	-rfnc)
	    RFNC=`echo $2 | tr [:lower:] [:upper:]`;
	    shift;
	    ;;
        esac;
        shift;
        done;
fi
function _print_stars(){
	# This function is to print stars.
	echo "******************************************"
}
Script_Start_Time=`date +%s`; # time to track when the script starts

# set home directory as work directory
cd ~

SCHEMA=`echo ${TABLE_NAME}|awk -F '.' '{print $1}'`
TABLE=`echo ${TABLE_NAME}|awk -F '.' '{print $2}'`
#echo $SCHEMA
#echo $TABLE
if [[ ${SCHEMA} == "" || ${TABLE} == "" ]]; then
	echo -e "\033[5m Error: \033[32m '"$TABLE_NAME"' \033[0mis not a valid table name in search path. \033[0m\n"
	Help
	exit 1;
fi

# convert table name to upper case
SCHEMA_NAME_IN_UPPER=`echo ${SCHEMA} | tr [:lower:] [:upper:]`;
TABLE_NAME_IN_UPPER=`echo ${TABLE} | tr [:lower:] [:upper:]`;
# get src_sys_cd
Param_SRC_SYS_CD=( $($sh/GET_RADA_PARAM.sh $sh/radar_hadoop.env ${ASSET_NAME} Param_SRC_SYS_CD) );
echo ""
_print_stars;
echo -e "\033[5m Searching constraint definitions ... \033[0m"
_print_stars;

if [[ ${KEY} == "" ]]; then
	key_type=""
else
	key_type=" and upper(constraint_type) = '"$KEY"'"
fi
if [[ ${RFNC} == "" ]]; then
	reference_table=""
else
	reference_table=" and upper(reference_table_name) = '"$RFNC"'"
fi
TMPFILE=$(mktemp AnalyzeConstraints.XXXXXXXXX)
echo "select concat_ws('^', constraint_type, constraint_name, nvl(cast (case when reference_table_name is null then null else reference_table_name end as string),'NULL'), concat_ws(',', collect_set(column_name)), nvl(cast (case when concat_ws(',',collect_set(reference_column_name)) is null then null else concat_ws(',',collect_set(reference_column_name)) end as string),'NULL')) from radar.constraint_columns where upper(table_schema)='"${SCHEMA_NAME_IN_UPPER}"' and upper(table_name)='"${TABLE_NAME_IN_UPPER}"'"${key_type}${reference_table}" group by constraint_type, constraint_name, reference_table_name;" >> $TMPFILE
echo -e "SQL: \033[31m select concat_ws('^', constraint_type, constraint_name, reference_table_name, concat_ws(',', collect_set(column_name)), concat_ws(',', collect_set(reference_column_name))) from radar.constraint_columns where upper(table_schema)='"${SCHEMA_NAME_IN_UPPER}"' and upper(table_name)='"${TABLE_NAME_IN_UPPER}"'"${key_type}${reference_table}" group by constraint_type, constraint_name, reference_table_name \033[0m";
SQL_CONSTRAINT=`hive --config hive-site.xml -S -f $TMPFILE`
SQL_CONSTRAINT=`echo ${SQL_CONSTRAINT} | tr " " "^";`
#// sample output:
#// f	FK1_ADDR	CTRY_CTY	CTRY_CTY_ID,SRC_SYS_CD
#// f	FK2_ADDR	CTRY_PSTL	CTRY_CD,CTRY_PSTL_CD,SRC_SYS_CD
rm $TMPFILE

constraint_values=""

OLD_IFS="$IFS"
IFS="^"
arr=($SQL_CONSTRAINT)
IFS="$OLD_IFS"
for ((i=0;i<${#arr[@]};i+=5))
do
	constraint_type=${arr[i]}
	constraint_name=${arr[i+1]}
	reference_table_name=${arr[i+2]}
	reference_keys=${arr[i+3]}
	reference_column_name=${arr[i+4]}

	if [ "${constraint_type}" = "p" ]; then
		echo -e "Primary Key : \033[32m "${reference_keys}" \033[0m"
		PK_NUM=`echo ${reference_keys} | awk -F',' '{print NF}'`
		# column value could be blank space, so if we split pk values by " ", then result could be incorrect.(value offset)
		# for each column, we should cast it to string, then use concat function to connect them.
		PK_LIST_TEMP=""
		OLD_IFS="$IFS"
		IFS=","
		arrPK_LIST=($reference_keys)
		IFS="$OLD_IFS"
		x=1
		for str in ${arrPK_LIST[@]}
		do
			PK_LIST_TEMP+="nvl(cast (case when t."$str" is null then null else t."$str" end as string),'NULL')"
			if [ $x != ${#arrPK_LIST[@]} ]; then
				PK_LIST_TEMP+=","
			fi
			((x++))
		done
		
		PK_LIST_FORMATTED=`echo "concat_ws('^', ${PK_LIST_TEMP})"`;
		# for debug, remove later!
		# echo ${PK_LIST_FORMATTED}
		_print_stars;
		echo -e "\033[5m Analyzing PK redundent... \033[0m"
		_print_stars;
		TMPFILE1=$(mktemp AnalyzeConstraints.XXXXXXXXX)
		echo "select concat_ws('^', collect_set(A.s)) from ( select "${PK_LIST_FORMATTED}" as s from ( select "${reference_keys}" from "${SCHEMA}"."${TABLE}" where SRC_SYS_CD='"${Param_SRC_SYS_CD}"' group by "${reference_keys}" having count(1) > 1 )t limit 10 )A" >> $TMPFILE1
		echo -e "SQL: \033[31m select "${reference_keys}" from "${SCHEMA}"."${TABLE}" where SRC_SYS_CD='"${Param_SRC_SYS_CD}"' group by "${reference_keys}" having count(1) > 1 limit 10 \033[0m"
		_print_stars;
		SQL_CHK_PK=`hive --config hive-site.xml -S -f ${TMPFILE1}`
		#echo $TMPFILE1
		#exit 0
		rm $TMPFILE1
		if [ X"${SQL_CHK_PK}" = X ]; then
			echo -e "\033[5m No PK issue found! \033[0m"
		else
			PK_RECORD=`echo ${SQL_CHK_PK}`
			i=0
			echo -e "\033[33m -[  RECORD  ]---+---------------- \033[0m"
			echo -e "\033[33m Schema Name     | "${SCHEMA}" \033[0m"
			echo -e "\033[33m Table Name      | "${TABLE}" \033[0m"
			echo -e "\033[33m Column Names    | "${reference_keys}" \033[0m"
			echo -e "\033[33m Constraint Name | "${constraint_name}" \033[0m"
			echo -e "\033[33m Constraint Type | PRIMARY \033[0m"
			echo -e "\033[33m ----------------+---------------- \033[0m"
			PK_DELIMITER_NUM=`echo ${PK_RECORD} | awk -F'^' '{print NF-1}'`
			if [[ $PK_RECORD =~ "^" ]]; then
				VALUE=""
				while ((1==1))
				do
					s=$(($i+1))
					split=`echo $PK_RECORD|cut -d "^" -f$s`
					VALUE+="\"$split\""
					#echo $i","$PK_DELIMITER_NUM
					if [[ $i != $PK_DELIMITER_NUM ]]; then
						((i++))
						if [ $(($i % $PK_NUM)) != 0 ]; then
							VALUE+=","
							continue
						else
							if [ $i -eq $PK_NUM ]; then
								echo -e "\033[33m Column Values   | ("${VALUE}") \033[0m"
							else
								echo -e "\033[33m                 | ("${VALUE}") \033[0m"
							fi
							constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'p', 'NULL', '(${VALUE})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
							VALUE=""
						fi
					else
						echo -e "\033[33m                 | ("${VALUE}") \033[0m"
						constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'p', 'NULL', '(${VALUE})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
						break
					fi
				done
			else
				echo -e "\033[33m Column Values   | ('"${PK_RECORD}"') \033[0m"
				constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'p', 'NULL', '(${PK_RECORD})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
			fi
		fi
	elif [ "${constraint_type}" = "u" ]; then
		UK_NUM=`echo ${reference_keys} | awk -F',' '{print NF}'`
		echo -e "Unique Key : \033[32m "${reference_keys}" \033[0m"
		UK_LIST_TEMP=""
		OLD_IFS="$IFS"
		IFS=","
		arrUK_LIST=($reference_keys)
		IFS="$OLD_IFS"
		x=1
		for str in ${arrUK_LIST[@]}
		do
			UK_LIST_TEMP+="nvl(cast (case when t."$str" is null then null else t."$str" end as string),'NULL')"
			if [ $x != ${#arrUK_LIST[@]} ]; then
				UK_LIST_TEMP+=","
			fi
			((x++))
		done

		UK_LIST_FORMATTED=`echo "concat_ws('^', ${UK_LIST_TEMP})"`;
		# for debug, remove later!
		#echo ${UK_LIST_FORMATTED}

		_print_stars;
		echo -e "\033[5m Analyzing UK redundent... \033[0m"
		_print_stars;
		TMPFILE2=$(mktemp AnalyzeConstraints.XXXXXXXXX)
		echo "select concat_ws('^', collect_set(A.s)) from ( select "${UK_LIST_FORMATTED}" as s from ( select "${reference_keys}" from "${SCHEMA}"."${TABLE}" where SRC_SYS_CD='"${Param_SRC_SYS_CD}"' group by "${reference_keys}" having count(1) > 1 )t limit 10 )A" >> $TMPFILE2
		echo -e "SQL: \033[31m select "${reference_keys}" from "${SCHEMA}"."${TABLE}" where SRC_SYS_CD='"${Param_SRC_SYS_CD}"' group by "${reference_keys}" having count(1) > 1 limit 10 \033[0m"
		SQL_CHK_UK=`hive --config hive-site.xml -S -f ${TMPFILE2}`
		rm $TMPFILE2
		_print_stars;
		if [ X"${SQL_CHK_UK}" = X ]; then
			echo -e "\033[5m No UK issue found! \033[0m"
		else
			UK_RECORD=`echo ${SQL_CHK_UK}`
			# for debug, remove later!
			#echo "begin-"$UK_RECORD"-end"
			l=0
			#OLD_IFS="$IFS"
			#IFS=","
			#arrUK=($UK_RECORD)
			#IFS="$OLD_IFS"
	
			echo -e "\033[33m -[  RECORD  ]---+---------------- \033[0m"
			echo -e "\033[33m Schema Name     | "${SCHEMA}" \033[0m"
			echo -e "\033[33m Table Name      | "${TABLE}" \033[0m"
			echo -e "\033[33m Column Names    | "${reference_keys}" \033[0m"
			echo -e "\033[33m Constraint Name | "${constraint_name}" \033[0m"
			echo -e "\033[33m Constraint Type | UNIQUE \033[0m"
			echo -e "\033[33m ----------------+---------------- \033[0m"
			UK_DELIMITER_NUM=`echo ${UK_RECORD} | awk -F'^' '{print NF-1}'`
			if [[ $UK_RECORD =~ "^" ]]; then
				VALUE=""
				while ((1==1))
				do
					s=$(($l+1))
					split=`echo $UK_RECORD|cut -d "^" -f$s`
					VALUE+="\"$split\""
					#echo $l","$UK_DELIMITER_NUM
					if [[ $l != $UK_DELIMITER_NUM ]]; then
						((l++))
						if [ $(($l % $UK_NUM)) != 0 ]; then
							VALUE+=","
							continue
						else
							if [ $l -eq $UK_NUM ]; then
								echo -e "\033[33m Column Values   | ("${VALUE}") \033[0m"
							else
								echo -e "\033[33m                 | ("${VALUE}") \033[0m"
							fi
							constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'u', 'NULL', '(${VALUE})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
							VALUE=""
						fi
					else
						echo -e "\033[33m                 | ("${VALUE}") \033[0m"
						constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'u', 'NULL', '(${VALUE})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
						break
					fi
				done
			else
					echo -e "\033[33m Column Values   | ('"${UK_RECORD}"') \033[0m"
					constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'u', 'NULL', '(${UK_RECORD})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
			fi
		fi
	elif [ "${constraint_type}" = "n" ]; then
		NN_NUM=`echo ${reference_keys} | awk -F',' '{print NF}'`
		_print_stars;
		echo -e "NULL Key : \033[32m "${reference_keys}" \033[0m"
		NN_LIST_TEMP=""
		OLD_IFS="$IFS"
		IFS=","
		arrNN_LIST=($reference_keys)
		IFS="$OLD_IFS"
		y=1
		NULL_CONDITION=""
		SOURCE_COLUMN_NAME=""
		for str1 in ${arrNN_LIST[@]}
		do
			SOURCE_COLUMN_NAME+=${TABLE_NAME_IN_UPPER}"."$str1
			NULL_CONDITION+=${TABLE_NAME_IN_UPPER}"."$str1" is null"
			if [ "$str1" = "RADAR_UPD_TS" ]; then
				NN_LIST_TEMP+="case when RADAR_UPD_TS is null then 'NULL' else from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') end"
			else
				NN_LIST_TEMP+="nvl(cast (case when "$str1" is null then 'NULL' else "$str1" end as string),'NULL')"
			fi
			if [ $y != ${#arrNN_LIST[@]} ]; then
				NN_LIST_TEMP+=","
				NULL_CONDITION+=" or "
				SOURCE_COLUMN_NAME+=","
			fi
			((y++))
		done
		NN_LIST_FORMATTED=`echo "concat_ws('^', ${NN_LIST_TEMP})"`;
		_print_stars;
		echo -e "Analyzing NotNull issue..."
		_print_stars;
		TMPFILE3=$(mktemp AnalyzeConstraints.XXXXXXXXXX)
		echo "select concat_ws('^', collect_set(A.s)) from ( select "${NN_LIST_FORMATTED}" as s from "${SCHEMA}"."${TABLE}" where "${TABLE_NAME_IN_UPPER}".SRC_SYS_CD='"${Param_SRC_SYS_CD}"' and ( "${NULL_CONDITION}" ) limit 10 )A" >> $TMPFILE3
		echo -e "SQL: \033[31m select distinct "${SOURCE_COLUMN_NAME}" from "${SCHEMA}"."${TABLE}" where "${TABLE_NAME_IN_UPPER}".SRC_SYS_CD='"${Param_SRC_SYS_CD}"' and ( "${NULL_CONDITION}" ) limit 10 \033[0m"
		_print_stars;
		SQL_CHK_NN=`hive --config hive-site.xml -S -f $TMPFILE3`
		rm -f $TMPFILE3
		# for debug,remove later!
		#echo $SQL_CHK_NN
		#exit 1
		if [ X"${SQL_CHK_NN}" = X ]; then
			echo -e "\033[5m No NN issue found! \033[0m"
		else
			NN_RECORD=`echo ${SQL_CHK_NN}`
			#| tr " " ",";`
			# for debug, remove later!
			#echo "begin-"$PK_RECORD"-end"
			k=0
			echo -e "\033[33m -[  RECORD  ]---+---------------- \033[0m"
			echo -e "\033[33m Schema Name     | "${SCHEMA}" \033[0m"
			echo -e "\033[33m Table Name      | "${TABLE}" \033[0m"
			echo -e "\033[33m Column Names    | "${reference_keys}" \033[0m"
			echo -e "\033[33m Constraint Name | "${constraint_name}" \033[0m"
			echo -e "\033[33m Constraint Type | NULL \033[0m"
			echo -e "\033[33m ----------------+---------------- \033[0m"
			#echo $NN_RECORD
			DELIMITER_NUM=`echo ${NN_RECORD} | awk -F'^' '{print NF-1}'`
			if [[ $NN_RECORD =~ "^" ]]; then
				VALUE=""
				while ((1==1))
				do
					a=$(($k+1))
					split=`echo $NN_RECORD|cut -d "^" -f$a`
					VALUE+="\"$split\""
					#echo $VALUE
					#if [ "$split" != "" ]; then
					if [[ $k != $DELIMITER_NUM ]]; then
						#       echo $k","$DELIMITER_NUM
						((k++))
						#VALUE="\""$VALUE"\""
						if [ $(($k % $NN_NUM)) != 0 ]; then
							VALUE+=","
							continue
						else
							if [ $k -eq $NN_NUM ]; then
								echo -e "\033[33m Column Values   | ("${VALUE}") \033[0m"
							else
								echo -e "\033[33m                 | ("${VALUE}") \033[0m"
							fi
							constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'n', 'NULL', '(${VALUE})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
							VALUE=""
						fi
					else
						echo -e "\033[33m                 | ("${VALUE}") \033[0m"
						constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'n', 'NULL', '(${VALUE})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
						break
					fi
				done
			else
				echo -e "\033[33m Column Values   | ('"${NN_RECORD}"') \033[0m"
				constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'n', 'NULL', '(${NN_RECORD})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
			fi
		fi
 	elif [ "${constraint_type}" = "f" ]; then
		_print_stars;
		echo -e "Foreign Key : \033[32m "${reference_keys}" \033[0m, reference table: \033[32m "${reference_table_name}" \033[0m, constraint name: \033[32m ${constraint_name} \033[0m"
		if [[ ${TABLE_NAME_IN_UPPER} == ${reference_table_name} ]]; then
			echo -e "\033[5m Expression returned a reference to target table itself, skipping... \033[0m"
			continue
		else
		FK_NUM=`echo ${reference_keys} | awk -F',' '{print NF}'`
		SOURCE_COLUMN_LIST_TEMP=""
		OLD_IFS="$IFS"
		IFS=","
		arrSOURCE_COLUMN_LIST=($reference_keys)
		arrTARGET_COLUMN_LIST=($reference_column_name)
		IFS="$OLD_IFS"
  		y=1
		JOIN_CONDITION=""
		NULL_CONDITION=""
		SOURCE_COLUMN_NAME=""
		RFNC_COLUMN_NAME=""
		for str1 in ${arrSOURCE_COLUMN_LIST[@]}
		do		
			SOURCE_COLUMN_NAME+=${TABLE_NAME_IN_UPPER}"."$str1
			RFNC_COLUMN_NAME+=${reference_table_name}"."${arrTARGET_COLUMN_LIST[y-1]}
			JOIN_CONDITION+=${TABLE_NAME_IN_UPPER}"."$str1"="${reference_table_name}"."${arrTARGET_COLUMN_LIST[y-1]}
			NULL_CONDITION+=${TABLE_NAME_IN_UPPER}"."$str1" is not null"
			SOURCE_COLUMN_LIST_TEMP+="nvl(cast (case when t."$str1" is null then 'NULL' else t."$str1" end as string),'NULL')"
			if [ $y != ${#arrSOURCE_COLUMN_LIST[@]} ]; then
				SOURCE_COLUMN_LIST_TEMP+=","
				JOIN_CONDITION+=" and "
				NULL_CONDITION+=" and "
				SOURCE_COLUMN_NAME+=","
				RFNC_COLUMN_NAME+=","
			fi
			((y++))
		done
		SOURCE_COLUMN_LIST_FORMATTED=`echo "concat_ws('^', ${SOURCE_COLUMN_LIST_TEMP})"`;

		_print_stars;
		echo -e "Analyzing FK issue for reference table \033[32m "${reference_table_name}" \033[0m ..."
		_print_stars;
		TMPFILE=$(mktemp AnalyzeConstraints.XXXXXXXXXX)
		echo "select concat_ws('^', collect_set(A.s)) from ( select "${SOURCE_COLUMN_LIST_FORMATTED}" as s from (select distinct "${SOURCE_COLUMN_NAME}" from "${SCHEMA}"."${TABLE}" where not exists ( select distinct "${RFNC_COLUMN_NAME}" from "${SCHEMA}"."${reference_table_name}" where "${reference_table_name}".SRC_SYS_CD='"${Param_SRC_SYS_CD}"' and "${JOIN_CONDITION}" ) and "${NULL_CONDITION}" and "${TABLE_NAME_IN_UPPER}".SRC_SYS_CD='"${Param_SRC_SYS_CD}"' )t limit 10 )A" >> $TMPFILE
		echo -e "SQL: \033[31m select distinct "${SOURCE_COLUMN_NAME}" from "${SCHEMA}"."${TABLE}" where not exists ( select distinct "${RFNC_COLUMN_NAME}" from "${SCHEMA}"."${reference_table_name}" where "${reference_table_name}".SRC_SYS_CD='"${Param_SRC_SYS_CD}"' and "${JOIN_CONDITION}" ) and "${NULL_CONDITION}" and "${TABLE_NAME_IN_UPPER}".SRC_SYS_CD='"${Param_SRC_SYS_CD}"' limit 10 \033[0m"
		_print_stars;
		SQL_CHK_FK=`hive --config hive-site.xml -S -f $TMPFILE`
		rm -f $TMPFILE
		# for debug,remove later!
		#echo $SQL_CHK_FK
		if [ X"${SQL_CHK_FK}" = X ]; then
			echo -e "\033[5m No FK issue found! \033[0m"
		else
			FK_RECORD=`echo ${SQL_CHK_FK}`
			#| tr " " ",";`
			# for debug, remove later!
			#echo "begin-"$PK_RECORD"-end"
			k=0
			echo -e "\033[33m -[  RECORD  ]---+---------------- \033[0m"
			echo -e "\033[33m Schema Name     | "${SCHEMA}" \033[0m"
			echo -e "\033[33m Table Name      | "${TABLE}" \033[0m"
			echo -e "\033[33m Reference Table | "${reference_table_name}" \033[0m"
			echo -e "\033[33m Column Names    | "${reference_keys}" \033[0m"
			echo -e "\033[33m Constraint Name | "${constraint_name}" \033[0m"
			echo -e "\033[33m Constraint Type | FOREIGN \033[0m"
			echo -e "\033[33m ----------------+---------------- \033[0m"
			#echo $FK_RECORD
			DELIMITER_NUM=`echo ${FK_RECORD} | awk -F'^' '{print NF-1}'`
			if [[ $FK_RECORD =~ "^" ]]; then
				VALUE=""
				while ((1==1))
				do
					a=$(($k+1))
					split=`echo $FK_RECORD|cut -d "^" -f$a`
					VALUE+="\"$split\""
					#echo $VALUE
					#if [ "$split" != "" ]; then
					if [[ $k != $DELIMITER_NUM ]]; then
						#	echo $k","$DELIMITER_NUM
						((k++))
						#VALUE="\""$VALUE"\""
						if [ $(($k % $FK_NUM)) != 0 ]; then
							VALUE+=","
							continue
						else
							if [ $k -eq $FK_NUM ]; then
								echo -e "\033[33m Column Values   | ("${VALUE}") \033[0m"
							else
								echo -e "\033[33m                 | ("${VALUE}") \033[0m"
							fi
							constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'f', '${reference_table_name}', '(${VALUE})',from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),  '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
							VALUE=""
						fi
					else
						echo -e "\033[33m                 | ("${VALUE}") \033[0m"
						constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'f', '${reference_table_name}', '(${VALUE})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
						break
					fi
				done
			
			else
				echo -e "\033[33m Column Values   | ('"${FK_RECORD}"') \033[0m"
				constraint_values+="select '${SCHEMA}', '${TABLE}', '${reference_keys}', '${constraint_name}', 'f', '${reference_table_name}', '(${FK_RECORD})', from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), '${Param_SRC_SYS_CD}', '${TABLE}' from radar.dual\n"
			fi
		fi
	fi
fi
done

if [ X"${constraint_values}" = X ]; then
	echo ""
else
	TMPFILEConstr1=$(mktemp AnalyzeConstraints.XXXXXXXXXX)
	TMPFILEConstr2=$(mktemp AnalyzeConstraints.XXXXXXXXXX)
	echo -e "insert overwrite table radar.constraints partition(src_sys_cd,table_name)\n${constraint_values}">>${TMPFILEConstr1};
	i=0
	numlines=`awk '{print NR}' $TMPFILEConstr1|tail -n1`
	while read line
	do
		((i++))
		if [[ $i != 1 && $i -lt $(($numlines - 1)) ]]; then
			echo -e $line" union all" >> $TMPFILEConstr2
		else
			echo -e $line >> $TMPFILEConstr2
		fi
	done < $TMPFILEConstr1
	_print_stars;
	echo -e "\033[5m Inserting constraint values into Hive ... \033[0m"
	_print_stars;
	hive --config hive-site.xml -f $TMPFILEConstr2 --verbose
	rm $TMPFILEConstr1
	rm $TMPFILEConstr2
fi

_print_stars;
Script_Finish_Time=`date +%s`;
echo -e "\033[5m Done. \033[0m"
time=$(($Script_Finish_Time-$Script_Start_Time))
hour=$(( $time / 3600 ))
minute=$(( ($time - $hour * 3600) / 60 ))
second=$(( $time % 60 ))
echo $hour" hours "$minute" minutes "$second" seconds spent on the whole script running."
