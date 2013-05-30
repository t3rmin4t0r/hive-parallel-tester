package org.notmysock.testify;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.notmysock.testify.CommandLineGenerator.FileWalker;

public class HiveParallelTester extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new HiveParallelTester(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        CommandLineParser parser = new BasicParser();
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption("a","antjar", true, "antjar");
        options.addOption("h","hivedir", true, "hivedir");
        options.addOption("o", "output", true, "output");
        options.addOption("m", "modules", true, "modules");
        options.addOption("p", "pertask", true, "pertask");
        options.addOption("c", "count", true, "count");
        options.addOption("r", "reportdir", true, "reportdir");
        options.addOption("m2", "maven2", true, "maven2");
        options.addOption("i2", "ivy2", true, "ivy2");
        CommandLine line = parser.parse(options, remainingArgs);

        if(!(line.hasOption("hivedir"))) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("HiveParalleTester", options);
          return 1;
        }
        
        
        File hivedir = new File(line.getOptionValue("hivedir"));
        File antjar = new File("ant-bin.zip");
        
        int pertask = 64;
        
        if(line.hasOption("pertask")) {
        	pertask = Integer.parseInt(line.getOptionValue("pertask")); 
        }
        
        if(line.hasOption("antjar")) {
        	antjar = new File(line.getOptionValue("antjar"));
        }
        
        String[] modules = null;
        if(line.hasOption("modules")) {
        	modules = line.getOptionValue("modules").split(",");	
        }
                
        CommandLineGenerator generator = new CommandLineGenerator(hivedir, modules);
        String[] commands = generator.getCommands(pertask, pertask);
        
        if(line.hasOption("count")) {
        	int count = Integer.parseInt(line.getOptionValue("c"));
        	String[] old = commands;
        	if(old.length > count) {
        		commands = new String[count];        		
        		for(int i = 0; i < count; i++) {
        			commands[i] = old[i];
        		}
        	}
        }
        File hivetar = makeTar(hivedir, new File("hive-build.tar"));
        File ivytar = null;
        File maventar = null;
        if(line.hasOption("ivy2")) {
            ivytar = makeTar(new File(line.getOptionValue("ivy2")), new File("ivy2.tar"));	
        } else {
        	File ivy2 = new File(System.getProperty("user.home"),".ivy2");
        	if(ivy2.exists() && ivy2.isDirectory()) {
            	ivytar = makeTar(ivy2, new File("ivy2.tar"));	
        	}
        }
        if(line.hasOption("maven2")) {
        	maventar = makeTar(new File(line.getOptionValue("maven2")), new File("maven2.tar"));
        } else {
        	File maven2 = new File(System.getProperty("user.home"),".maven2");
        	if(maven2.exists() && maven2.isDirectory()) {
            	maventar = makeTar(maven2, new File("maven2.tar"));	
        	}
        }

        Configuration conf = getConf(); 
        conf.setInt("mapred.task.timeout",0);
        conf.setInt("mapreduce.task.timeout",0);
/*
        conf.setInt("mapreduce.map.maxattempts", 1);
        conf.setInt("mapred.map.max.attempts", 1);
*/
        conf.setInt("mapred.job.map.memory.mb", 4096);
        
        ArrayList<Path> cacheFiles = new ArrayList<Path>();
        
        cacheFiles.addAll(Arrays.asList(new Path[] {
        		copyToHDFS(antjar), 
        		copyToHDFS(hivetar),
        		copyToHDFS(new File("epilogue.sh")),
        		copyToHDFS(new File("prologue.sh"))
        }));
        
        DistributedCache.addCacheArchive(pathToLink(cacheFiles.get(0),"ant"), conf);
        DistributedCache.addCacheFile(pathToLink(cacheFiles.get(1),"hive-build.tar"), conf);
        DistributedCache.addCacheFile(pathToLink(cacheFiles.get(2),"epilogue.sh"), conf);
        DistributedCache.addCacheFile(pathToLink(cacheFiles.get(3),"prologue.sh"), conf);
        
        if(ivytar != null) {
        	Path cachefile = copyToHDFS(ivytar);
        	cacheFiles.add(cachefile);
        	DistributedCache.addCacheFile(pathToLink(cachefile,"ivy2.tar"), conf);
        }
        if(maventar != null) {
        	Path cachefile = copyToHDFS(maventar);
        	cacheFiles.add(cachefile);
        	DistributedCache.addCacheFile(pathToLink(cachefile,"m2.tar"), conf);
        }

        DistributedCache.createSymlink(conf);

        
        Path in = genInput(commands);
        long epoch = System.currentTimeMillis() / 1000;
		Path out = new Path("/tmp/hive-test-out-" + epoch);
		if(line.hasOption("output")) {
			out = new Path(line.getOptionValue("output"));
		}
        
        
        Job job = new Job(conf, "hive-parallel-test");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(0);
        job.setMapperClass(AntCommandRunner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 1);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // use multiple output to only write the named files
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "text", 
          TextOutputFormat.class, LongWritable.class, Text.class);

        boolean success = job.waitForCompletion(true);

        // cleanup
        FileSystem fs = FileSystem.get(getConf());
        
        Path reports = new Path(out, "reports");
        
        ReportMaker reporter = new ReportMaker(fs);
        String reportdir = "report/";
        if(line.hasOption("reportdir")) {
        	reportdir = line.getOptionValue("reportdir");
        }
        reporter.create(reports, new File(reportdir));
        
        fs.delete(in, false);
        for(Path tmpjar: cacheFiles) {
        	fs.delete(tmpjar, false);
        }
        
        System.out.println("Reports generated in: "+reportdir);
        System.out.println("Raw output in HDFS: " + out.toString());
        
        parseStatusFiles(fs, out);
        
        return 0;
    }
    
    private void parseStatusFiles(FileSystem fs, Path out) 
    		throws IOException {
    	FileStatus[] files = fs.listStatus(out);
        
        ArrayList<InputStream> streams = new ArrayList<InputStream>();
        for(FileStatus f: files) {
        	if(f.getPath().getName().startsWith("part-m-")) {
        		streams.add(fs.open(f.getPath()));
        	}
        }
        
		BufferedReader linereader = new BufferedReader(new InputStreamReader(
				new SequenceInputStream(Collections.enumeration(streams))));
       		int i = 0, j =0; 
		String line = null;
		while((line = linereader.readLine()) != null) {
			i++;
			if(!line.contains("\tOK")) {
				j++;
				System.err.println(line);
			}
		}
		System.out.printf("Ran %d tasks (%d errors)\n", i, j);
    }
    
    private URI pathToLink(Path in, String symlink) throws Exception {
    	URI uri = in.toUri();
        URI link = new URI(uri.getScheme(),
                    uri.getUserInfo(), uri.getHost(), 
                    uri.getPort(),uri.getPath(), 
                    uri.getQuery(),symlink);
        return link;     
    }
    
    private static String progress(int percent)
    {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < 100; i++) {
        	sb.append( i <= percent ? "=" : "_");
        }
        sb.append("|"+percent+"%");
        return sb.toString();
    }
    
    public File makeTar(File dir, File tar) throws Exception {
    	String cmd = String.format("tar -cf %s .", tar.getCanonicalPath());
    	if(tar.exists()) {
    		cmd = String.format("tar -uf %s .", tar.getCanonicalPath());
    	}    	
    	Runtime runtime = Runtime.getRuntime();
    	Process p = runtime.exec(cmd, null, dir);
    	int status = p.waitFor();
    	if(status != 0) {
    		throw new IOException("Could not create "+tar.getName()+" from: "+cmd+" in "+dir.getPath());
    	}
		return tar;
    } 
    
	public Path copyToHDFS(File jar) throws Exception {
		Random rand = new Random();
		BigInteger md5 = new BigInteger(64, rand);
		String md5hex = md5.abs().toString(16);
		Path dst = new Path(String.format("/tmp/%s-%s", md5hex, jar.getName()));

		System.out.println(dst);
		Path src = new Path(jar.toURI());
		FileSystem fs = FileSystem.get(getConf());
		fs.copyFromLocalFile(false, /* overwrite */true, src, dst);
		return dst;
	}

	public Path genInput(String[] commands) throws Exception {
		long epoch = System.currentTimeMillis() / 1000;
		Path in = new Path("/tmp/hive-test-" + epoch);
		FileSystem fs = FileSystem.get(getConf());
		FSDataOutputStream out = fs.create(in);
		for (String s : commands) {
			out.writeBytes(s + "\n");
		}
		out.close();
		FileOutputStream debug = new FileOutputStream(new File("commands.txt"));
		for (String s : commands) {
			debug.write((s + "\n").getBytes());
		}
		debug.close();
		return in;
	}

	public static final class AntCommandRunner extends
			Mapper<LongWritable, Text, Text, Text> {
		private MultipleOutputs mos;
		String bashCommand="source prologue.sh; source epilogue.sh; prologue; %s; epilogue";

		static String readToString(InputStream in) throws IOException {
			InputStreamReader is = new InputStreamReader(in);
			StringBuilder sb = new StringBuilder();
			BufferedReader br = new BufferedReader(is);
			String read = br.readLine();

			while (read != null) {
				// System.out.println(read);
				sb.append(read);
				read = br.readLine();
			}
			return sb.toString();
		}

		protected void setup(Context context) throws IOException {
			mos = new MultipleOutputs(context);		
		}
		
		private void copyFile(MultipleOutputs mos, File f, String name) 
				throws IOException, InterruptedException {
			if(!f.exists()) return;
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line;
			while ((line = br.readLine()) != null) {
				// process the line.
				mos.write("text", line, null, name);
			}
			br.close();
			f.deleteOnExit();
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}

		protected void map(LongWritable offset, Text command,
				Mapper.Context context) throws IOException,
				InterruptedException {
			OutputStream cmdFile = new FileOutputStream(new File("exec.sh"));
			cmdFile.write(String.format(bashCommand, command).getBytes());
			cmdFile.flush();
			cmdFile.close();
			String cmd = "/bin/bash ./exec.sh";
			String[] envp = new String[2];
			envp[0]="PATH="+System.getenv("PATH");
			envp[1]="JAVA_HOME="+System.getenv("JAVA_HOME");
			Process p = Runtime.getRuntime().exec(cmd, envp,new File("."));
			TailerListener echoer = new TailerListenerAdapter() {
				@Override
				public void handle(String line) {
					System.out.println(line);
				}
			};
			Tailer tail = Tailer.create(new File("stdout"), echoer);
			Executors.newSingleThreadExecutor().execute(tail);
			int status = p.waitFor();
			
			copyFile(mos, new File("stdout"), "stdout");
			copyFile(mos, new File("exec.sh"), "exec.sh");
			FileWalker fw = new FileWalker("TEST.*.xml$");
			fw.walk(new File("."));
			for(File f: fw.getMatches()) {
				copyFile(mos, f, "reports/"+f.getName());
			}
			
			/*if (status != 0) {
				String err = readToString(p.getErrorStream());
				if(new File("stderr").exists()) {
					err += readToString(new FileInputStream("stderr"));	
				}
				throw new InterruptedException(
						"Process failed with status code " + status + "\n"
								+ err);
			}*/
			context.write(command, status == 0 ? "OK" : "FAIL");
			tail.stop();
		}
	}
}
