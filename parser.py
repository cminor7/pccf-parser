import multiprocessing
from os import path, makedirs

# Function to process a chunk of lines
def process_lines(lines):
	result = []
	for line in lines:
		list_line = [
			line[0:6],  # postal_code
			line[6:9],  # fsa
			line[9:11], # province_code
			line[11:15],# cd_uid
			line[15:22],# csd_uid
			line[22:92].strip(), # csd_name
			line[92:95].strip(), # csd_type
			line[95:98],  # ccs_code
			line[98:101], # sac
			line[101:102],# sac_type
			line[102:109].strip(), # ct_name
			line[109:111], # econ_region
			line[111:115], # designated_place_code
			line[115:120], # fed13_uid
			line[120:124], # pop_cntr_ra
			line[124:125], # pop_cntr_ra_type
			line[125:133], # da_uid
			line[133:136], # dissemination_block
			line[136:137], # rep_pt_type
			line[137:148].strip(), # latitude
			line[148:161].strip(), # longitude
			line[161:162], # sli
			line[162:163], # pc_type
			line[163:193].strip(), # comm_name
			line[193:194], # dmt
			line[194:195], # h_dmt
			line[195:203], # birth_date
			line[203:211], # retire_date
			line[211:212], # po
			line[212:215], # quality_indicator
			line[215:216], # source
			line[216:217]  # pop_cntr_ra_size_class
		]
		result.append(','.join(list_line))
	return result

# Function to split the file into chunks
def chunkify(file_name, chunk_size=2000):
	with open(file_name, 'r') as file:
		lines = file.readlines()
	# Split the lines into chunks
	return [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

def write_output(output_data, output_file):
	with open(output_file, 'a') as f:
		for line_data in output_data:
			f.write(f"{line_data}\n")

# Main function
def main():
	if not path.exists('OUTPUT'):
		makedirs('OUTPUT')

	file_name = 'PCCF_FCCP_V2212_2021.txt'
	output_file = "OUTPUT/output.csv"
	
	# Write the header
	with open(output_file, 'w') as f:
		f.write("POSTAL_CODE,FSA,PROVINCE_CODE,CD_UID,CSD_UID,CSD_NAME,CSD_TYPE,CCS_CODE,SAC,SAC_TYPE,CT_NAME,ECON_REGION,DESIGNATED_PLACE_CODE,FED13_UID,POP_CNTR_RA,POP_CNTR_RA_TYPE,DA_UID,DISSEMINATION_BLOCK,REP_PT_TYPE,LATITUDE,LONGITUDE,SLI,PC_TYPE,COMM_NAME,DMT,H_DMT,BIRTH_DATE,RETIRE_DATE,PO,QUALITY_INDICATOR,SOURCE,POP_CNTR_RA_SIZE_CLASS\n")

	# Split the file into chunks
	chunks = chunkify(file_name)

	# Create a pool of workers to process chunks
	with multiprocessing.Pool() as pool:
		# Process each chunk in parallel
		results = pool.map(process_lines, chunks)

	# Write the processed data to the output file
	for result in results:
		write_output(result, output_file)

if __name__ == '__main__':
	main()