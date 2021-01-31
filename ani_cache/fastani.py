###############################################################################
#                                                                             #
#    This program is free software: you can redistribute it and/or modify     #
#    it under the terms of the GNU General Public License as published by     #
#    the Free Software Foundation, either version 3 of the License, or        #
#    (at your option) any later version.                                      #
#                                                                             #
#    This program is distributed in the hope that it will be useful,          #
#    but WITHOUT ANY WARRANTY; without even the implied warranty of           #
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the            #
#    GNU General Public License for more details.                             #
#                                                                             #
#    You should have received a copy of the GNU General Public License        #
#    along with this program. If not, see <http://www.gnu.org/licenses/>.     #
#                                                                             #
###############################################################################

import sys
import time
import datetime
import logging
import re
import subprocess
import sqlite3
import multiprocessing as mp
from collections import defaultdict

from ani_cache.exceptions import FastANIError
from ani_cache.utils import check_dependencies


class FastANI():
    """Calculate ANI between genomes using a precomputed cache where possible."""

    def __init__(self, ani_cache_file, cpus):
        """Initialization."""

        check_dependencies(['fastANI'])

        # do database insertions in batches to reduce number of transactions
        self.DB_BATCH_SIZE = 100

        self.cpus = cpus

        self.logger = logging.getLogger('timestamp')

        self.ani_db_file = ani_cache_file
        self._create_db()

        self.logger.info('Using FastANI v{}.'.format(self._get_version()))

    def _get_version(self):
        """Returns the version of FastANI on the system path.

        Returns
        -------
        str
            The string containing the FastANI version.
        """
        try:
            proc = subprocess.Popen(['fastANI', '-v'],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    encoding='utf-8')
            _stdout, stderr = proc.communicate()
            if stderr.startswith('Unknown option:'):
                return 'unknown (<1.3)'

            version = re.search(r'version (.+)', stderr)
            return version.group(1)
        except Exception as e:
            print(e)
            return 'unknown'

    def _create_db(self):
        """Read previously calculated ANI values."""

        if self.ani_db_file:
            self.logger.info(f'Connecting to ANI DB: {self.ani_db_file}')
            try:
                self.db_conn = sqlite3.connect(self.ani_db_file)
            except Exception as e:
                print(e)
                raise

            self.db_cur = self.db_conn.cursor()
            self.db_cur.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ani_table'")

            if not self.db_cur.fetchone():
                self.logger.info(f' - creating ANI database table.')

                self.db_cur.execute('''CREATE TABLE ani_table
                (query_id TEXT NOT NULL,
                ref_id TEXT NOT NULL,
                ani REAL NOT NULL,
                af REAL NOT NULL)''')

                self.db_cur.execute('CREATE INDEX gid_idx ON ani_table(query_id, ref_id)')

                self.db_conn.commit()
            else:
                self.logger.info(f' - database contains {self.num_db_rows():,} entries')
        else:
            self.logger.info('Not using an ANI database.')
            self.db_conn = None

    def write_cache(self):
        """Write cache to file."""

        if self.db_conn:
            self.logger.info('Commiting new results to database:')
            self.logger.info(f' - database contains {self.num_db_rows():,} entries')
            self.db_conn.commit()
            self.db_conn.close()

    def num_db_rows(self):
        """Get number of rows in the database."""

        if self.db_conn:
            self.db_cur.execute("SELECT max(rowid) from ani_table")
            return self.db_cur.fetchone()[0]

        return 0

    def fastani(self, qid, rid, q_gf, r_gf):
        """Calculate,ANI between a pair of genomes."""

        # run FastANI and write results to stdout
        try:
            cmd = ['fastANI', '-q', q_gf, '-r', r_gf, '-o', '/dev/stdout']
            proc = subprocess.Popen(cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    encoding='utf-8')
            stdout, stderr = proc.communicate()

            if proc.returncode != 0:  # FastANI returned an error code
                print(stderr)
                raise FastANIError(f"FastANI exited with code {proc.returncode}.")
        except Exception as e:
            print(e)
            raise

        result_tokens = stdout.split()
        if len(result_tokens) == 5:
            ani = float(result_tokens[2])
            af = float(result_tokens[3])/int(result_tokens[4])
        elif not result_tokens:
            ani = 0.0
            af = 0.0
        else:
            raise FastANIError(f'Unexpected stdout from FastANI: {stdout}')

        ani_af = (qid, rid, ani, af)

        return ani_af

    def __fastani_worker(self, ani_db_file, genomic_files, queue_in, queue_out):
        """Process each data item in parallel."""

        db_cur = None
        if ani_db_file:
            db_conn = sqlite3.connect(self.ani_db_file)
            db_cur = db_conn.cursor()

        while True:
            qid, rid = queue_in.get(block=True, timeout=None)
            if qid is None:
                break

            in_db = False
            if db_cur:
                # check if entry is in database
                db_cur.execute('SELECT ani, af FROM ani_table WHERE query_id=? AND ref_id=?', (qid, rid,))
                db_ani_af = db_cur.fetchone()
                if db_ani_af:
                    in_db = True
                    ani, af = db_ani_af
                    ani_af = (qid, rid, ani, af)

            if not in_db:
                # calculate ANI between genomes
                ani_af = self.fastani(qid, rid,
                                      genomic_files[qid],
                                      genomic_files[rid])

            queue_out.put((in_db, ani_af))

        # send message indicating process is done
        queue_out.put(None)

    def _check_cache(self, gid_pairs):
        """Check if all ANI comparisons are already in the cache."""

        ani_af = defaultdict(dict)
        for qid, rid in gid_pairs:
            self.db_cur.execute('SELECT ani, af FROM ani_table WHERE query_id=? AND ref_id=?', (qid, rid,))
            db_ani_af = self.db_cur.fetchone()

            if db_ani_af:
                ani, af = db_ani_af
                ani_af[qid][rid] = (ani, af)
            else:
                return None

        return ani_af

    def pairs(self, gid_pairs, genome_files, report_progress=True, initial_cache_check=False):
        """Calculate FastANI between specified genome pairs in parallel."""

        # check if all pairs are already in cache
        if initial_cache_check:
            ani_af = self._check_cache(gid_pairs)
            if ani_af:
                return ani_af

         # performing calculations in parallel has the expense of setting
         # up the queues and spawning processes. For small numbers of
         # # comparisons it is better to just use a single CPU.
        if len(gid_pairs) <= 6:
            d = defaultdict(dict)
            for qid, rid in gid_pairs:
                qid, rid, ani, af = self.fastani(qid, rid,
                                                 genome_files[qid],
                                                 genome_files[rid])
                d[qid][rid] = (ani, af)

            self.db_conn.commit()  # commit new ANI values to database

            return d

        # calculate ANI between genomes in parallel
        worker_queue = mp.Queue()
        writer_queue = mp.Queue()

        for gid1, gid2 in gid_pairs:
            worker_queue.put((gid1, gid2))

        for _ in range(self.cpus):
            worker_queue.put((None, None))

        try:
            worker_proc = [mp.Process(target=self.__fastani_worker, args=(self.ani_db_file,
                                                                          genome_files,
                                                                          worker_queue,
                                                                          writer_queue)) for _ in range(self.cpus)]

            for p in worker_proc:
                p.start()

            processed_genome = 0
            finished_processes = 0
            ani_af = defaultdict(dict)
            db_ani_af = []
            start_time = time.time()
            while finished_processes != len(worker_proc):
                rtn = writer_queue.get(block=True, timeout=None)
                if rtn is None:
                    finished_processes += 1
                    continue

                in_db, (qid, rid, ani, af) = rtn
                ani_af[qid][rid] = (ani, af)

                if not in_db and self.db_conn:
                    # save result for insertion into database
                    db_ani_af.append((qid, rid, ani, af))

                    if len(db_ani_af) == self.DB_BATCH_SIZE:
                        # place results into database in batches to keep
                        # the number of transactions sensible
                        self.db_cur.executemany('INSERT INTO ani_table (query_id, ref_id, ani, af) VALUES (?, ?, ?, ?)',
                                                db_ani_af)
                        self.db_conn.commit()

                        db_ani_af = []

                if report_progress:
                    processed_genome += 1
                    elapsed_time = time.time() - start_time
                    remaining_time = (elapsed_time/processed_genome)*float(len(gid_pairs) - processed_genome)

                    status = '-> Processing {:,} of {:,} ({:.2f}%) genome pairs [elapsed: {}, remaining: {}].'.format(
                        processed_genome,
                        len(gid_pairs),
                        float(processed_genome*100)/len(gid_pairs),
                        str(datetime.timedelta(seconds=round(elapsed_time))),
                        datetime.timedelta(seconds=round(remaining_time)))
                    sys.stdout.write('\r\033[K')  # clear line
                    sys.stdout.write(f'{status}')
                    sys.stdout.flush()

            if db_ani_af:
                # place final set of results into database
                self.db_cur.executemany('INSERT INTO ani_table (query_id, ref_id, ani, af) VALUES (?, ?, ?, ?)',
                                        db_ani_af)
                self.db_conn.commit()

            if report_progress:
                sys.stdout.write('\n')
        except Exception as e:
            print(e)
            for p in worker_proc:
                p.terminate()
            raise

        return ani_af

    @staticmethod
    def symmetric_ani(ani_af, gid1, gid2):
        """Calculate symmetric ANI statistics between genomes."""

        if (gid1 not in ani_af
                or gid2 not in ani_af
                or gid1 not in ani_af[gid2]
                or gid2 not in ani_af[gid1]):
            return 0.0, 0.0

        cur_ani, cur_af = ani_af[gid1][gid2]
        rev_ani, rev_af = ani_af[gid2][gid1]

        # ANI should be the larger of the two values as this
        # is the most conservative circumscription and reduces the
        # change of creating polyphyletic species clusters
        ani = max(rev_ani, cur_ani)

        # AF should be the larger of the two values in order to
        # accomodate incomplete and contaminated genomes
        af = max(rev_af, cur_af)

        return ani, af

    @staticmethod
    def mean_ani(ani_af, gid1, gid2):
        """Calculate mean ANI statistics between genomes."""

        if (gid1 not in ani_af
                or gid2 not in ani_af
                or gid1 not in ani_af[gid2]
                or gid2 not in ani_af[gid1]):
            return 0.0, 0.0

        cur_ani, cur_af = ani_af[gid1][gid2]
        rev_ani, rev_af = ani_af[gid2][gid1]

        ani = (rev_ani + cur_ani) / 2
        af = (rev_af + cur_af) / 2

        return ani, af

    def write_full_matrix(self, output_file, ani_af, ani_or_af):
        """Write full matrix with ANI or AF results."""

        sorted_gids = sorted(ani_af)

        fout = open(output_file, 'w')
        fout.write('\t{}\n'.format('\t'.join(sorted_gids)))
        for gid1 in sorted_gids:
            fout.write(gid1)

            for gid2 in sorted_gids:
                ani, af = ani_af[gid1][gid2]
                if ani_or_af:
                    v = ani
                else:
                    v = af

                fout.write('\t{}'.format(v))
            fout.write('\n')
        fout.close()

    def write_ani_matrix(self, output_file, ani_af):
        """Write full matrix with ANI results."""

        self.write_full_matrix(output_file, ani_af, ani_or_af=True)

    def write_af_matrix(self, output_file, ani_af):
        """Write full matrix with AF results."""

        self.write_full_matrix(output_file, ani_af, ani_or_af=False)

    def write_ani_af(self, output_file, ani_af):
        """Write out ANI and AF results."""

        fout = open(output_file, 'w')
        fout.write('Query\tReference\tANI\tAF\n')
        for qid in ani_af:
            for rid in ani_af[qid]:
                ani, af = ani_af[qid][rid]
                fout.write('{}\t{}\t{}\t{}\n'.format(
                    qid,
                    rid,
                    ani,
                    af))
        fout.close()
