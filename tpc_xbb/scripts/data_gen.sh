#!/bin/bash

#Commandline usage:
#java -jar pdgf.jar  <Parameters>
#
#Available Commands/command line parameters:
#-ShortCOMMAND  PARAMETERS
#-LongCOMMAND   DESCRIPTION
#-----------------------------------------------
#-a            --none--
#-abort        Stops a running data generation
#
#-ap           <interval_ms>(default 1000, 0 to turn off)
#-autoProgress Enable continuous generation progress print.
#
#-c            <true/1/false/0>(optional)
#-closeWhenDone If Activated, automatically exits the program once a running data generation finished. If no parameter is specified 'true' is assumed.
#
#-q            --none--
#-exit         Stops a running data generation and gracefully exits the program
#
#-h            --none--
#-help         Displays all available commands with a short description and their parameters
#
#-lj           <dir> OR <class/jar>
#-libjars      If <dir>: add provided path to the lookup locations for plugins. Path is searched recursively. Multiple paths can be linked together with ';': e.g.: libs/;plugins/. Alternatively if path is a file, load only the specified class/jar file.
#
#-license      --none--
#              Show the end user license agreement
#
#-lp           <propertyName> OR <formula>
#-listProperty Displays all defined <property name="propertyName"> and their value, including properties set as via the 'setProperty' command. If <propertyName> parameter is specified, list only specified property. Property discovery uses the same engine as the the properties them self. You can use this command to test out advanced properties e.g. logarithmic scaling of the default ScalingFactor: listProperty 1+Math.log(${SF})*1000
#
#-ns           --none--
#-noShell      Disables interactive mode. No internal command shell will be available during program runtime. This is best suited for unattended batch or cluster mode. You can issue every shell command as startup argument. Deactivating the shell also activates automatic program termination after a data generation run. Just like the closeWhenDone command.
#
#-nc           <number>
#-nodeCount    Cluster generation mode - split generation onto multiple compute nodes. Total number of participating data Generation Nodes
#
#-nn           <number>
#-nodeNumber   Cluster generation mode - split generation onto multiple compute nodes. Node number/ID [1, nodeCount] of this Data Generation Node
#
#-o            <outputPath>
#-output       String literals must be enclosed in ticks: 'foo'. If your path contains whitespace, enclose it in "". Example 1: -o "'/home/foo bar/'" to set path to: /home/foo bar/. You can access certain internal variables. Available: project, table, tableID, timeID, output, nodeCount, nodeNumber, outputDir, fileEnding Example 2: -o "'/home/foo/Batch' + timeID + '/'" to set path to: /home/foo/BatchX/ and X=timeID
#
#-#            <d> (optional)
#-printProgress Display generation progress. Add argument 'd' for 'detailed' to display more information
#
#-sp           <name> <valueOrFormula>
#-setProperty  Set a new or override a existing <property>.
#Examples:
#setProperty myProperty 5.0
#setProperty myProperty 1+Math.log(${SF})*1000
#
#-sf           <sf>
#              Default scale factor ${SF} for the project. The property is mostly used to dynamically increase a tables size based on a formula. e.g.: '1000*${SF}'. By increasing the scale factor you also increase the number of generated rows of a table.
#
#-s            <tableName> <tableName:timeIDToStart>...(optional)
#-start        Starts data generation on this node. Optionally you can limit the data generation to the specified table(s). All others tables will be excluded. If the timelineSimulator system is used, you can also add starting timeID to the table(s) name(s). Example to only generate the customer table starting at timeID 5: 's Customer:5'
#
#-verbosity    <none>; <LogLevel>(optional);  <appenderName> <LogLevel>(optional)
#-v            Increases or decreases verbosity. Without parameters: enables more verbose mode. Optionally you can specify the desired verbosity level from less to more: OFF, FATAL, ERROR, WARN, INFO(default), DEBUG, TRACE, ALL
#
#-w            <number>
#-workers      Controls parallelity of the data generation process. Sets the number of used threads/workers. If not specified the program automatically uses as many data generation threads as auto detected number of CPU core's
#
#Note: some commands depend on other commands. For example: you can not start the datageneration if the config files are not loaded.

SCALE_F=1
PDGF_DIR="$HOME/romeo/git/TPCx-BB-kit-code-1.4.0/data-generator"
DATA_DIR="$HOME/romeo/bigbench"
DATA_GEN_WORKERS=1

for PARTS in 1 2 4 8; do
  echo "partitions $PARTS"

  for i in $(seq 1 $PARTS); do
    /usr/bin/java -jar "$PDGF_DIR"/pdgf.jar -nc "$PARTS" -nn "$i" -ns -c -sp REFRESH_PHASE 0 -o "'$DATA_DIR/$PARTS/data/'+table.getName()+'/'" -workers $DATA_GEN_WORKERS -ap 3000 -s -sf $SCALE_F
  done

  for i in $(seq 1 $PARTS); do
    /usr/bin/java -jar "$PDGF_DIR"/pdgf.jar -nc "$PARTS" -nn "$i" -ns -c -sp REFRESH_PHASE 1 -o "'$DATA_DIR/$PARTS/data_refresh/'+table.getName()+'/'" -workers $DATA_GEN_WORKERS -ap 3000 -s -sf $SCALE_F
  done

  echo "partitions $PARTS done!"
  echo "======================="
done
