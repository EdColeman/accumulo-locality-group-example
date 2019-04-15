# Generating Sample Data.

Using a test class that uses the Accumulo mini-cluster, generated a table with sample data.  The
table was compacted, cloned and then locality groups added and then the clone was compacted, this
re-writes the table file with the locality group information.

Using the rfile-info utility, the original table looks like:

> Reading File: /opt/accumulo/accumulo-current/bin/accumulo rfile-info file:./accumulo/test/target/mini-tests/org.apache.accumulo.test.functional.RfileLgDevIT_t1/accumulo/tables/1/default_tablet/
  A0000004.rf
>
>RFile Version            : 8

> Locality group           : <DEFAULT>
	Num   blocks           : 5
	Index level 0          : 166 bytes  1 blocks
	First key              : 000 G1_COL1:qual [] 1555270070601 false
	Last key               : fff G5_COL1:qual [] 1555270070601 false
	Num entries            : 28,672
	Column families        : [G4_COL1, G1_COL2, G1_COL1, G5_COL1, G2_COL1, G2_COL2, G2_COL3]
>
> Meta block     : BCFile.index
      Raw size             : 4 bytes
      Compressed size      : 12 bytes
      Compression type     : gz
>
>Meta block     : RFile.index
      Raw size             : 365 bytes
      Compressed size      : 195 bytes
      Compression type     : gz

The clone table adds three locality groups, the names were chosen to show any sorting that happens. From the column
families the prefix G1, G2 is used to assign to the locality groups z_group_1 and a_group_2 respectively. The
prefixs G4 and G5 are unassigned and the na_group does not have any column families in the generated data.
 
| Group Name |    Column Families        |
| -----------|:-------------------------:|
| z_group_1  | G1_COL1, G1_COL2          | 
| a_group_2  | G2_COL1, G2_COL2, G2_COL3 |
| na_group   | none                      |
|            | G4_COL1, G5_COL1          |

After compaction, rfile-info reports the following structure:

>Reading file: file:./accumulo/test/target/mini-tests/org.apache.accumulo.test.functional.RfileLgDevIT_t1/accumulo/tables/2/c-00000000/A0000005.rf
2019-04-14 21:10:22,177 [conf.ConfigSanityCheck] WARN : Use of instance.dfs.uri and instance.dfs.dir are deprecated. Consider using instance.volumes instead.
RFile Version            : 8
>
>Locality group           : z_group_1
	Num   blocks           : 2
	Index level 0          : 74 bytes  1 blocks
	First key              : 000 G1_COL1:qual [] 1555270070601 false
	Last key               : fff G1_COL2:qual [] 1555270070601 false
	Num entries            : 8,192
	Column families        : [G1_COL2, G1_COL1]
Locality group           : na_group
	Num   blocks           : 0
	Index level 0          : 0 bytes  1 blocks
	First key              : null
	Last key               : null
	Num entries            : 0
	Column families        : [na]
Locality group           : a_group_2
	Num   blocks           : 2
	Index level 0          : 77 bytes  1 blocks
	First key              : 000 G2_COL1:qual [] 1555270070601 false
	Last key               : fff G2_COL3:qual [] 1555270070601 false
	Num entries            : 12,288
	Column families        : [G2_COL1, G2_COL2, G2_COL3]
Locality group           : <DEFAULT>
	Num   blocks           : 2
	Index level 0          : 67 bytes  1 blocks
	First key              : 000 G4_COL1:qual [] 1555270070601 false
	Last key               : fff G5_COL1:qual [] 1555270070601 false
	Num entries            : 8,192
	Column families        : [G4_COL1, G5_COL1]
>
>Meta block     : BCFile.index
      Raw size             : 4 bytes
      Compressed size      : 12 bytes
      Compression type     : gz
>
>Meta block     : RFile.index
      Raw size             : 596 bytes
      Compressed size      : 251 bytes
      Compression type     : gz
