package org.notmysock.testify;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;

public class CommandLineGenerator {

	private final File hivedir;
	private HashMap<String,ArrayList<String>> tests = new HashMap<String,ArrayList<String>>();
	private ArrayList<String> queries = new ArrayList<String>();
	private String[] drivers = new String[] { "TestParse.java",
			"TestParseNegative.java", "TestCliDriver.java",
			/*"TestBeeLineDriver.java",*/ "TestMinimrCliDriver.java",
			"TestNegativeMinimrCliDriver.java", "TestNegativeCliDriver.java", };
	private String[] excluded = new String[] { "TestSerDe.java",
			"TestHiveMetaStore.java", "TestBeeLineDriver.java",
			"TestHiveServer2Concurrency.java", };

	public static class FileWalker {
		private Pattern fnmatch;
		private ArrayList<File> list = new ArrayList<File>();
		public FileWalker(String fnmatch) {
			this(Pattern.compile(fnmatch));
		}
		public FileWalker(Pattern fnmatch) {
			this.fnmatch = fnmatch;
			this.list.clear();
		}
		public void walk(File root) {
			File[] list = root.listFiles();
			for (File f : list) {
				if (f.isDirectory()) {
					walk(f);
				} else {
					if(fnmatch == null || fnmatch.matcher(f.getName()).matches()) {
						this.list.add(f);
					}
				}
			}
		}
		public ArrayList<File> getMatches() {
			return this.list;
		}
	}
	
	private HashMap<String,ArrayList<String>> getTests(String[] modules) throws IOException {		
		HashMap<String,ArrayList<String>> tests = new HashMap<String, ArrayList<String>>();

		FileWalker fw = new FileWalker("Test.*\\.java$");
		fw.walk(hivedir);
		String basepath = hivedir.getCanonicalPath()+"/";
		for(File f: fw.getMatches()) {
			if(ArrayUtils.indexOf(excluded,f.getName()) != -1) {
				continue;
			}
			if(ArrayUtils.indexOf(drivers,f.getName()) != -1) {
				continue;
			}
			if(f.getName().indexOf("$") != -1) {
				continue;
			}
			String fullpath = f.getCanonicalPath().replace(basepath,"");
			if(fullpath.startsWith("build/")) fullpath = fullpath.replace("build/", "");
			String[] directories = fullpath.split("/");
			String module = directories[0];

			if(modules != null && ArrayUtils.indexOf(modules, module) == -1) {
				continue;
			}
			ArrayList<String> l = tests.get(module);
			if(l == null) {
				l = new ArrayList<String>();
			}
			l.add(f.getName().replace(".java",""));
			tests.put(module, l);
		}
		return tests;
	}
	
	private ArrayList<String> getQueries(String[] modules) {

		ArrayList<String> queries = new ArrayList<String>();
		if(modules != null && ArrayUtils.indexOf(modules, "ql") == -1) {
			return queries;
		}
		FileWalker fw = new FileWalker(".*\\.q$");
		fw.walk(new File(hivedir, "ql"));
		for(File f: fw.getMatches()) {
			queries.add(f.getName().replace(".q",""));
		}
		return queries;
	}
    
	public CommandLineGenerator(File hivedir, String[] modules) throws IOException {
		this.hivedir = hivedir;
		this.queries = getQueries(modules);
		this.tests = getTests(modules);
	}
	
	private String join(List<String> l, String seperator) {
		StringBuilder builder = new StringBuilder();
		builder.append(l.remove(0));
		for( String s : l) {
		    builder.append( seperator);
		    builder.append(s);
		}
		return builder.toString();
	}
	
	public String[] getCommands(int testPerLine, int queriesPerLine) {
		ArrayList<String> commands = new ArrayList<String>();
		ArrayList<String> temp = new ArrayList<String>();
		String ant = "ant test $TESTARGS";
		for(String module: tests.keySet()) {
			ArrayList<String> moduletests = tests.get(module); 
			String separator = ".class,**/";
			if("hcatalog".equals(module)) {
				separator = ".java,**/";
			}
			for (int i = 0; i < moduletests.size(); i++) {
				temp.add(moduletests.get(i));
				if (((i + 1) % testPerLine) == 0) {
					commands.add(String.format(ant + " -Dtestcase=%s -Dmodule=%s",
							join(temp, separator), module));
					temp.clear();
				}
			}
			if (temp.size() != 0) {
				commands.add(String.format(ant + " -Dtestcase=%s -Dmodule=%s",
						join(temp, separator), module));
			}
		}
		for(int i = 0; i < drivers.length; i++) {
			temp.clear();
			for(int j = 0; j < queries.size(); j++) {
				temp.add(queries.get(j));
				if (((j + 1) % queriesPerLine) == 0) {
					commands.add(String.format(ant
							+ "-Dmodule=ql -Dtestcase=%s -Dqfile_regex='(%s)'",
							drivers[i], join(temp, "|")));
					temp.clear();
				}
			}
			if (temp.size() != 0) {
				commands.add(String.format(ant
						+ "-Dmodule=ql -Dtestcase=%s -Dqfile_regex='(%s)'",
						drivers[i], join(temp, "|")));
			}
		}
		//return new String[]{commands.get(0),commands.get(1),commands.get(2)};
		return commands.toArray(new String[0]);
	}
}
