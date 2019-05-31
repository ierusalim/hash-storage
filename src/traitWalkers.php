<?php
namespace HashStorage;

trait traitWalkers
{
    public function restoreHashesDb($dbg = true)
    {
        $rows_cnt =
            $this->walkHashesByFile(function($hash_n, $hash_bin) {
                if (!$hash_n) {
                    $this->db_hashes->exec("BEGIN");
                } elseif (!($hash_n % 1000)) {
                    $this->db_hashes->exec("COMMIT");
                    $this->db_hashes->exec("BEGIN");
                }
                $hash_hex = bin2hex($hash_bin);
                $this->db_hashes->exec("INSERT INTO hashes(hash) VALUES (x'$hash_hex')");
                return 0;
            }, 0, $dbg);
        $this->db_hashes->exec("COMMIT");
        return $rows_cnt;
    }
    public function restoreHashesFile($dbg = true)
    {
        return
            $this->walkHashesByDb(function($hash_n, $hash_bin) {
                $rec_n = $this->f_hashes->appendRecord($hash_bin);
                if ($rec_n != $hash_n) {
                    throw new \Exception("Syncronization damaged");
                }
                return 0;
            }, 0, $dbg);
    }
    public function checkHashes($from_num = 0, $dbg = true) {
        $chk = $this->walkHashesByFile(function($hash_n, $hash_bin) {
            $hash_hex = bin2hex($hash_bin);
            $rec_n = $this->getHashNum($hash_hex);
            if ($hash_n != $rec_n) return 1;
            return 0;
        }, $from_num, $dbg);
    }
    public function walkHashesByDb($fn_call, $from_num = 0, $dbg = true)
    {
        $db = $this->db_hashes;
        // Count records
        $have_cnt = $db->query('SELECT COUNT (*) FROM hashes');
        $have_cnt = $have_cnt->fetchArray(SQLITE3_NUM);
        $have_cnt = $have_cnt[0];

        if (!$have_cnt) {
            if ($dbg) {
                echo "No hashes in db.\n";
            }
            return 0;
        }
        if ($dbg) {
            echo "Walking $have_cnt hashes by db from #$from_num to #$have_cnt\n";
        }
        $all_rec = $db->query("SELECT rowid, hash FROM hashes WHERE rowid>$from_num");
        while ($row = $all_rec->fetchArray(SQLITE3_NUM)) {
            $hash_n = $row[0] - 1;
            $hash_bin = $row[1];

            if (call_user_func($fn_call, $hash_n, $hash_bin)) break;

            if ($dbg) {
                if (!($hash_n % 100)) echo $hash_n . ' ';
                if (!($hash_n % 1000)) echo "\n";
            }
        }
        return $hash_n + 1;
    }
    public function walkHashesByFile($fn_call, $from_num = 0, $dbg = true)
    {
        $fr = $this->f_hashes;
        // Count records
        $have_cnt = $fr->recordsCount();
        if (!$have_cnt) {
            if ($dbg) {
                echo "No hashes in file\n";
            }
            return 0;
        }
        if ($dbg) {
            echo "Walking $have_cnt hashes by hashes-file from #$from_num to #$have_cnt\n";
        }
        for($hash_n=$from_num; $hash_n < $have_cnt; $hash_n++) {
            $hash_bin = $fr->readRecord($hash_n);

            if (call_user_func($fn_call, $hash_n, $hash_bin)) break;

            if ($dbg) {
                if (!($hash_n % 100)) echo $hash_n . ' ';
                if (!($hash_n % 1000)) echo "\n";
            }
        }
        return $hash_n + 1;
    }
}
