require 'pp'

#run from the setup directory
#Usage: > ruby setup_impala.rb [function]

#This ruby script is to setup your Impala environment.
#It will write to HDFS and execute commands through the Impala shell.
#Be aware that you may need to modify your permissions

#First, check ARGV for a "function" option, which will 
#try to recreate the median function
function = false
if (ARGV.length >= 1 and ARGV[0] == "function")
	pp "will attempt to create median function"
	function = true
end

#Now, check that all the scripts that are needed exist in this directory
#set HDFS_LOCATION to be the full path
HDFS_LOCATION = '/user/hive/udfs/'
required_files = {}
required_files['tables'] = 'setup_for_ruby.txt'
required_files['function'] = 'libudasample.so'

required_files.keys.each do |x|
	unless (File.exist?(required_files[x]))
		pp "#{required_files[x]} is not in this directory"
		exit 0
	end
end

puts "\nFYI, to setup the median function for your impala manually, " +
	"run the following commands: "
puts "hadoop fs -mkdir /user/hive/udfs"
puts "hadoop fs -put ../uda/build/libudasample.so /user/hive/udfs"
puts "hadoop fs -chmod 777 /user/hive/udfs"
puts "hadoop fs -chmod 667 /user/hive/udfs/*"
puts "hadoop fs -chown hive /user/hive/udfs/*"
puts "In impala, run:"
puts "create aggregate function median(double) returns string location '/user/hive/udfs/libudasample.so' init_fn='AvgInit' update_fn='AvgUpdate' merge_fn='AvgMerge' finalize_fn='AvgFinalize';\n\n"

puts "Checking for base directory..."
existing_files = `hadoop fs -ls #{HDFS_LOCATION}/*`

#If the base directory doesn't exist, create it, open permissions and set function = true
if (existing_files.empty?)
	dir_structure = HDFS_LOCATION.split("/")
	path = ""
	if (dir_structure[0].empty?)
		path = "/"
	end
	puts "creating #{HDFS_LOCATION}"
	dir_structure.each do |d|
		next if (d.empty?)
		path += "#{d}/"
		puts "running: hadoop fs -mkdir #{path}..."
		`hadoop fs -mkdir #{path}`
	end
	`hadoop fs -chmod -R 777 #{HDFS_LOCATION}`
	function = true
end

found_files = {}
existing_files.each_line do |l|
	fields = l.chomp.split(/\s+/)
	next if (fields.length < 8)
	#for each file, remove HDFS_LOCATION and see what's left
	file = fields[7]
	filename = file.sub(HDFS_LOCATION,"")
	#not expecting any files in child directories, so there should be no /'s at this point
	next if (filename.count("/") != 0)
	found_files[filename] = file
end

#check that all required files are present
files_needed = []
required_files.keys.each do |x|
	next if (x == "tables")
	if (!found_files.has_key?(required_files[x]))
		files_needed << required_files[x]
	end
end

#for each file needed, copy to HDFS
files_needed.each do |x|
	puts "creating #{x}..."
	`hadoop fs -put #{x} #{HDFS_LOCATION}/`
end

#chmod and chown new files
if (files_needed.length >= 1)
	puts "making new files chmod 666..."
	`hadoop fs -chmod 666 #{HDFS_LOCATION}/*`
	`hadoop fs -chown hive #{HDFS_LOCATION}/*`
end

#if function, create function
if (function)
	puts "creating function..."
	`impala-shell -q "create aggregate function median(double) returns string location '#{HDFS_LOCATION}/#{required_files['function']}' init_fn='AvgInit' update_fn='AvgUpdate' merge_fn='AvgMerge' finalize_fn='AvgFinalize'"`
end

#run script to (re)create rollup tables
puts "(re)creating rollup tables..."
puts `impala-shell -f #{required_files['tables']}`
