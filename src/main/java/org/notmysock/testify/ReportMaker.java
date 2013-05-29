package org.notmysock.testify;

import java.io.File;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Target;
import org.apache.tools.ant.taskdefs.optional.junit.AggregateTransformer;
import org.apache.tools.ant.taskdefs.optional.junit.AggregateTransformer.Format;
import org.apache.tools.ant.taskdefs.optional.junit.XMLResultAggregator;
import org.apache.tools.ant.types.FileSet;

import com.google.common.io.Files;

public class ReportMaker {
	private FileSystem fs;
	public ReportMaker(FileSystem fs) { 
		this.fs = fs;		
	}
	
	public void create (Path inputdir, File outputdir)
			throws Exception {
		File xmldir = downloadAll(inputdir);
		generateReport(xmldir, outputdir);
	}
		
	
	private File downloadAll(Path inputdir) throws Exception {
		 File tempdir = Files.createTempDir();
		 FileStatus[] files = fs.listStatus(inputdir);
		 for(int i = 0; i < files.length; i++) {
			 FileStatus fd = files[i];
			 Path dst = new Path(new Path(tempdir.getAbsolutePath()),i+".xml");
			 fs.copyToLocalFile(false, fd.getPath(), dst);
		 }
		 return tempdir;
	}
	
	private void generateReport(File xmldir, File outputdir) {
		Project p = new Project();
		p.init();
		Target t = new Target();
		FileSet fs = new FileSet();
		fs.setDir(xmldir);
		fs.createInclude().setName("*.xml");
		XMLResultAggregator aggregator = new XMLResultAggregator();
		aggregator.addFileSet(fs);
		AggregateTransformer transformer = aggregator.createReport();
		outputdir.mkdirs();
		transformer.setTodir(outputdir);
		t.addTask(aggregator);
		aggregator.setProject(p);
		t.setProject(p);
		p.addTarget("report",t);
		p.executeTarget("report");
	}
	
	public static void main(String[] args) throws Exception {
		FileSystem fs = new RawLocalFileSystem();
		ReportMaker rm = new ReportMaker(null);
		rm.generateReport(new File(args[0]), new File(args[1]));
	}
}
