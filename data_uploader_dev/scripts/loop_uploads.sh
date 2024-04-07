#!/bin/bash
############################################################
# Help                                                     #
############################################################
Help()
{
   echo "Loops over the same dataset and re-uploads it using new UIDs"
   echo
   echo "Syntax: loop_uploads [-c|s|a|d|n|h]"
   echo "options:"
   echo "c     Annalise Backend Client ID."
   echo "s     Annalise Backend Client Secret."
   echo "a     Annalise Backend Client base url."
   echo "d     Folder to search for DICOM files."
   echo "n     Number of times to loop over the data set"
   echo "h     Prints this help."
   echo
}

############################################################
# Main
############################################################
while getopts ":hc:s:a:d:n:" option; do
   case $option in
      h) # display Help
         Help
         exit;;
      c) client_id=$OPTARG;;
      s) client_secret=$OPTARG;;
      a) api_host=$OPTARG;;
      d) dicom_directory=$OPTARG;;
      n) loops=$OPTARG;;
      \?) # Invalid option
         echo "Error: Invalid option"
         exit;;
   esac
done

for ((i = 0 ; i < $loops ; i++)); do
    poetry run python data_uploader/upload_dcms.py  -c $client_id -s $client_secret -a $api_host -d $dicom_directory
done