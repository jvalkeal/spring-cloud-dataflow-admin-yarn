/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.dataflow.server.yarn.shell;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.table.AbsoluteWidthSizeConstraints;
import org.springframework.shell.table.BorderSpecification;
import org.springframework.shell.table.BorderStyle;
import org.springframework.shell.table.CellMatchers;
import org.springframework.shell.table.Formatter;
import org.springframework.shell.table.Table;
import org.springframework.shell.table.TableBuilder;
import org.springframework.shell.table.TableModel;
import org.springframework.stereotype.Component;

/**
 * Shell hdfs related commands.
 *
 * @author Janne Valkealahti
 *
 */
@Component
public class HdfsCommands implements CommandMarker {

	private static final String PREFIX = "hadoop fs ";
	private static final String FALSE = "false";
	private static final String TRUE = "true";
	private static final String RECURSIVE = "recursive";
	private static final String RECURSION_HELP = "whether with recursion";

	private FsShell shell;

	@Autowired
	public void setFsShell(FsShell shell) {
		this.shell = shell;
	}

	@CliCommand(value = PREFIX + "ls", help = "List files in the directory")
	public Table ls(
			@CliOption(key = { "", "dir" }, mandatory = false, unspecifiedDefaultValue = ".", help = "directory to be listed") final String path,
			@CliOption(key = { RECURSIVE }, mandatory = false, specifiedDefaultValue = TRUE, unspecifiedDefaultValue = FALSE, help = RECURSION_HELP) final boolean recursive) {
		Collection<FileStatus> files = recursive ? shell.lsr(path) : shell.ls(path);
		return applySimpleListStyle(new TableBuilder(new FileStatusTableModel(files.toArray(new FileStatus[0])))).build();
	}

	private static TableBuilder applySimpleListStyle(TableBuilder builder) {
		builder
			.paintBorder(BorderStyle.air, BorderSpecification.INNER_VERTICAL)
				.fromTopLeft().toBottomRight()
			.on(CellMatchers.column(4))
				.addSizer(new AbsoluteWidthSizeConstraints(19))
				.addFormatter(new TableDateTimeFormatter());
		return builder;
	}

	private static class TableDateTimeFormatter implements Formatter {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		public String[] format(Object value) {
			String formatted = "";
			if (value instanceof Long) {
				formatted = sdf.format(new Date((Long)value));
			}
			return new String[] { formatted };
		}
	}

	private static class FileStatusTableModel extends TableModel {

		private final FileStatus[] statuses;

		public FileStatusTableModel(FileStatus[] statuses) {
			this.statuses = statuses;
		}

		@Override
		public int getRowCount() {
			return statuses.length;
		}

		@Override
		public int getColumnCount() {
			return 6;
		}

		@Override
		public Object getValue(int row, int column) {
			if (column == 0) {
				return statuses[row].getPermission().toString();
			} else if (column == 1) {
				return statuses[row].getOwner();
			} else if (column == 2) {
				return statuses[row].getGroup();
			} else if (column == 3) {
				return statuses[row].getLen();
			} else if (column == 4) {
				return statuses[row].getModificationTime();
			} else if (column == 5) {
				return Path.getPathWithoutSchemeAndAuthority(statuses[row].getPath());
			} else {
				return "";
			}
		}
	}
}
