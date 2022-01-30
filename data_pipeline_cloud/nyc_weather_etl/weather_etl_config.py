import sys

if len(sys.argv) != 5:
    raise Exception('Missing arguments, 4 needed (startyear,endyear,inputpath,outputpath.')

start_year = int(sys.argv[1])
end_year = int(sys.argv[2])
user_filepath = sys.argv[3]
output_filepath = sys.argv[4]
