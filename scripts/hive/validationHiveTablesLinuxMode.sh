#!/bin/bash

# script to do quick validation of Hive tables

# Documentation:
# https://community.hortonworks.com/articles/1283/hive-script-to-validate-tables-compare-one-with-an.html

listOfTables=$*

origDatabase=hiveSourceOfTruthDatabase
resultDatabase=hiveDatabasetoValidate

baseDir=/tmp/validation/${origDatabase}_${resultDatabase}
[ -d $baseDir ] || mkdir -p $baseDir
globalReport=$baseDir/globalReport
echo -e "\n\n######################################################### New analysis (`date`) #########################################################\n" >> $globalReport
echo -e "Arguments passed:\n$listOfTables\n" >> $globalReport

# If a file is bigger than splitNumLines, then we split it into chunks (each chunk having splitNumLines lines at most), in order to make easier the comparisons with vimdiff
splitNumLines=301024

for bucket in $listOfTables ; do

    splitMode=0

    # if there are exclusions, let's handle them here
    table=`echo $bucket | cut -d: -f1`
    # columnsToExclude should have the format: col24,col12,col3
    # if some columns have a "space", use '.' instead. For instance, for column "coa id" you would have to write "coa.id"
    columnsToExclude=`echo $bucket | cut -s -d: -f2 | sed 's/,/|/g' `

    echo -n "####  Comparing $origDatabase.$table with $resultDatabase.$table" | tee -a $globalReport
    if [ "x$columnsToExclude" = "x" ]; then
	echo | tee -a $globalReport
    else
	echo " with the following columns excluded: $columnsToExclude" | tee -a $globalReport
    fi

    communDir=$baseDir/$table
    origDir=$communDir/orig
    resultDir=$communDir/result
    [ -d $communDir/tmp ] || mkdir -p $communDir/tmp
    hiveExecuteFile_orig=$communDir/tmp/hiveExecute_orig
    hiveExecuteFile_result=$communDir/tmp/hiveExecute_result

    #####
    # Download all the table to the Linux file system
    #####

    # move dir if already exists. Only keep 1 history
    for mydir in $origDir $resultDir; do
	[ -d $mydir.old ] && rm -rf $mydir.old
	[ -d $mydir ] && mv $mydir $mydir.old
    done

    insert1stPart="INSERT OVERWRITE local directory"
    insert2stPart="ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' select"

    if [ "x$columnsToExclude" = "x" ]; then
    	echo "$insert1stPart '$origDir' $insert2stPart * from $origDatabase.$table;" > $hiveExecuteFile_orig
    	echo "$insert1stPart '$resultDir' $insert2stPart * from $resultDatabase.$table;" > $hiveExecuteFile_result
    else
    	echo "set hive.support.quoted.identifiers=none;" | tee $hiveExecuteFile_orig $hiveExecuteFile_result &> /dev/null
    	echo "$insert1stPart '$origDir' $insert2stPart " '`('$columnsToExclude')?+.+` ' "from $origDatabase.$table;" >> $hiveExecuteFile_orig
    	echo "$insert1stPart '$resultDir' $insert2stPart " '`('$columnsToExclude')?+.+` ' " from $resultDatabase.$table;" >> $hiveExecuteFile_result
    fi

    queueConf='--hiveconf tez.queue.name=default'
    ( hive $queueConf -f $hiveExecuteFile_orig &> ${hiveExecuteFile_orig}.log ) &
    ( hive $queueConf -f $hiveExecuteFile_result &> ${hiveExecuteFile_result}.log ) &
    wait

    for tableDir in $origDir $resultDir; do
	if [ ! -d $tableDir ]; then
		echo "Was not able to find the path $tableDir. Are you sure you that the table $table is in the Hive database?" | tee -a $globalReport
		continue 2
	fi
    done

    #####
    # Sort the tables downloaded
    #####
    for i in $origDir $resultDir; do
	(
        cd $i
	echo $columnsToExclude > columnsToExclude

	sort -S 30% --temporary-directory=$communDir/tmp --numeric-sort 0* -o sorted
	) &
    done
    wait

    #####
    # Count the number of rows
    #####
    for i in $origDir $resultDir; do
	cd $i
	if [ $i = $origDir ]; then
		origTableNumLines=`wc -l $origDir/sorted | cut -d' ' -f1`
		[ $origTableNumLines -gt $splitNumLines ] && splitMode=1	# we only check the size of the orig table to decide if we need to split
	else
		resultTableNumLines=`wc -l $resultDir/sorted | cut -d' ' -f1`
	fi
    done

    for i in $origDir $resultDir; do
	(
	cd $i
	if [ "x$splitMode" = "x1" ]; then
		split --suffix-length=3 --lines=$splitNumLines sorted sorted-
		rm -f sorted
	fi

        rm -f 0* .0*crc   # remove unnecessary files in order to not fill the FileSystem
	) &
    done
    wait

    #####
    # Compare the results
    #####
    [ "x$origTableNumLines" != "x$resultTableNumLines" ] && echo "ERROR: the number of rows is different from orig table ($origTableNumLines rows) to result table ($resultTableNumLines rows)" | tee -a $globalReport
    if [ "x$splitMode" = "x1" ]; then
	numDiff=0
	chunksWithErrors=""
	for fileChunk in $origDir/sorted-* ; do
	    chunk=`basename $fileChunk`
	    [ ! -e $resultDir/$chunk ] && continue	# we might have no file if there is less data in result table than in orig
	    numDiffTmp=`diff {$origDir,$resultDir}/$chunk | grep '^<' | wc -l`
	    if [ $numDiffTmp -gt 0 ]; then
		((numDiff=$numDiff + $numDiffTmp))
		chunksWithErrors="${chunksWithErrors}$numDiffTmp\t\t\t$chunk\n"
	    else
		rm -f $resultDir/$chunk		# we don't need to replicate the same data
	    fi
	done
    else
	numDiff=0
	if [ -e $resultDir/sorted ]; then
	    grepDiffPattern='^<'
	    [ $origTableNumLines -lt $resultTableNumLines ] && grepDiffPattern='^>'
	    numDiff=`diff {$origDir,$resultDir}/sorted | grep $grepDiffPattern | wc -l`
	fi
	[ "x$numDiff" = "x0" ] && rm -f $resultDir/sorted	# we don't need to replicate the same data
    fi

    #####
    # Show the results
    #####
    echo 'Number of differences: ' $numDiff "(out of  $origTableNumLines rows)" | tee -a $globalReport

    if [ "x$numDiff" != "x0" ]; then
        if [ "x$splitMode" = "x1" ]; then
	    echo "To see the differences visually: vimdiff -o -c \"windo set wrap foldcolumn=0\" $communDir/{orig,result}/<chunkID>.gz"
	    echo "NumberOfDifferences	ChunkIK"
	    echo -e $chunksWithErrors
	else
	    echo "To see the differences visually: vimdiff -o -c \"windo set wrap foldcolumn=0\" $communDir/{orig,result}/sorted.gz"
    	    echo
	fi
    else
	echo
    fi

    gzip $origDir/sorted* &
    [ "x$numDiff" != "x0" ] && gzip $resultDir/sorted* &
done


