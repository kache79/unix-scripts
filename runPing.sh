#! /bin/bash 

#echo "" > results.txt
LINES=$(wc -l serverList.txt | awk -F " " '{print $1}') 
COUNTER=0
HOSTS=()
TIMINGS=()
while read HOSTNAME
do
	COUNTER=`expr $COUNTER + 1`
	AVGTIME=$(ping -c 3 -W 1 "$HOSTNAME" | tail -n 1 | awk -F "/" '{print $5}')
	if [ "$AVGTIME" == "" ]
	then
		AVGTIME=0
	fi
	TIMINGS[$COUNTER]="$AVGTIME"
	HOSTS[$COUNTER]="$HOSTNAME"	
	echo "$COUNTER/$LINES $AVGTIME $HOSTNAME"
	#echo "$AVGTIME $HOSTNAME" >> results.txt
done < serverList.txt

i=1
echo "" > results.txt
echo "" > goodList.txt
echo "" > badList.txt
echo -e "\n\n\n----------SORTED RESULTS----------\n"
while [ $i -le $COUNTER ]
do
	if (( $(echo "${TIMINGS[$i]} > 0" | bc -l) ))
	then
		echo "${TIMINGS[$i]} ${HOSTS[$i]}" >> results.txt
		echo "${HOSTS[$i]}" >> goodList.txt
	else
		echo "${HOSTS[$i]}" >> badList.txt
	fi
	((i++))
done

cat results.txt | sort -k1 -g 
