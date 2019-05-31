<?php
namespace HashStorage;
/**
 * $hs = new \HashStorage\HashStorage(path, hash-len);
 * $hs->addHash(hash) -- return next hash-number (local record id 0,1,2,...)
 * $hs->addData(hash-number, data-string)
 * $hs->addHashData(hash, data) -- addHash + addData -- return hash-number
 * $hs->getHashData(hash) -- return string data by hash
 * $hs->getData(hash-number) -- return string data by hash-number
 * $hs->getHashNum(hash) -- return hash-number or false if not exist
 * $hs->getHash(hash-number) -- return hash by hash-number
 * $hs->recordsCount() -- return count of storaged hashes
 *
 */

class HashStorage
{
    use traitWalkers;
    use traitDRS;

    public $base_path;
    public $trait;
    public $hash_len;

    public $f_hashes;
    public $db_hashes;

    /**
     * Return count of storaged records (count of storaged hashes)
     *
     * @param boolean $recount
     * @return integer
     */
    public function recordsCount($recount = false)
    {
        return $this->f_hashes->recordsCount($recount);
    }

    /**
     * Return hash-hex by specified local hash-number
     *
     * @param integer $hash_n
     * @return false|string contains hash in hex-format
     */
    public function getHash($hash_n, $ret_bin = false)
    {
        $fr = $this->f_hashes;
        $hash_ret = $fr->readRecord($hash_n);
        if (($hash_ret !== false) && !$ret_bin) {
            $hash_ret = bin2hex($hash_ret);
        }
        return $hash_ret;
    }

    /**
     * Search hash in local database.
     *
     * Return false if not found
     * Return local hash-number if found
     *
     * @param string $hash_hex Hash in hex format
     * @return false|integer
     */
    public function getHashNum($hash_hex_or_bin)
    {
        $l = strlen($hash_hex_or_bin);
        if ($l ==- $this->hash_len) {
            $hash_hex = bin2hex($hash_hex_or_bin);
        } elseif ($l / 2 === $this->hash_len) {
            $hash_hex = $hash_hex_or_bin;
        } else {
            throw new \Exception('Illegal hash length');
        }

        $db = $this->db_hashes;
        $check_rec = $db->query("SELECT rowid FROM hashes WHERE hash = (x'$hash_hex')");
        $check_rec = $check_rec->fetchArray(SQLITE3_NUM);
        if ($check_rec !== false) {
            $check_rec = $check_rec[0]-1;
        }
        return $check_rec;
    }

    public function addHash($hash_hex_or_bin)
    {
        $l = strlen($hash_hex_or_bin);
        if ($l === $this->hash_len) {
            $hash_bin = $hash_hex_or_bin;
            $hash_hex = bin2hex($hash_bin);
        } elseif ($l / 2 === $this->hash_len) {
            $hash_hex = $hash_hex_or_bin;
            $hash_bin = hex2bin($hash_hex);
        } else {
            throw new \Exception('Illegal hash length');
        }

        $db = $this->db_hashes;
        $fr = $this->f_hashes;

        // Check already exists
        $check_rec = $db->query("SELECT rowid FROM hashes WHERE hash = (x'$hash_hex')");
        $check_rec = $check_rec->fetchArray(SQLITE3_NUM);
        if ($check_rec !== false) {
            // hash already exists
            return false;
        }
        // hash not found

        // insert hash to hash-db
        if (!$db->exec("INSERT INTO hashes(hash) VALUES (x'$hash_hex')")) {
            throw new \Exception("Error hash insert to db");
        }
        $last_row_id = $db->lastInsertRowID();

        $hash_n = $last_row_id - 1;

        $expected_n = $this->recordsCount();

        if ($hash_n !== $expected_n) {
            // remove hash from database if got unexpected rowid
            $db->exec("DELETE FROM hashes WHERE hash = x'$hash_hex'");
            throw new \Exception("Unsynchronized: unexpected record number in db");
        }

        // append hash to hash-file
        $rec_n = $fr->appendRecord($hash_bin);

        if ($hash_n !== $rec_n) {
            throw new Exception("Unsynchronized hash-db and hash-file");
        }
        return $hash_n;
    }

    public function __construct($base_path, $hash_len_or_example, $trait = 'main', $start_repair = false)
    {
        //cheking base-path , must exist
        if(!is_dir($base_path)) {
            throw new \Exception("Not found base_path=$base_path");
        }
        if (substr($base_path, -1) !== DIRECTORY_SEPARATOR) {
            $base_path .= DIRECTORY_SEPARATOR;
        }
        $this->base_path = $base_path;

        // check hash
        if (is_integer($hash_len_or_example)) {
            $this->hash_len = $hash_len_or_example;
        } else {
            $hash_bin = @hex2bin($hash_len_or_example);
            if (empty($hash_bin)) {
                throw new \Exception("Hash must have hex-format");
            }
            $this->hash_len = strlen($hash_bin);
        }
        if (($this->hash_len < 4) || ($this->hash_len > 64)) {
            throw new \Exception("Hash must have fixed size 4..64 bytes");
        }

        if ((strlen($trait)<1) || (strlen($trait)>16)) {
            throw new \Exception("Trait-name len must be from 1 to 16");
        }
        $this->trait = $trait;

        $filename_db = $base_path . $trait . '-hs.db';
        $filename_hashes = $base_path . $trait . '-hs.bin';

        $db = new \SQLite3($filename_db);
        $db->exec('PRAGMA synchronous = OFF');
        $db->exec('PRAGMA temp_store = MEMORY');

        // Create table if not exists
        $res = $db->exec("CREATE TABLE IF NOT EXISTS hashes(hash BLOB PRIMARY KEY)");
        if ($res === false) {
            throw new \Exception("Bad database file: $filename_db");
        }

        // Count records
        $have_cnt = $db->query('SELECT COUNT (*) FROM hashes');
        $have_cnt = $have_cnt->fetchArray(SQLITE3_NUM);
        $have_cnt = $have_cnt[0];

        $this->db_hashes = $db;

        $this->f_hashes = new \ierusalim\FileRecords\FileRecords($filename_hashes, $this->hash_len);
        $hashes_cnt = $this->f_hashes->recordsCount();

        if ($hashes_cnt != floor($hashes_cnt)) {
            $err_msg = "File $filename_hashes not corresponded with hash_len=" . $this->hash_len;
            if ($have_cnt) {
                $err_msg .= "\nTo restore damaged file try ->restoreHashesFile()";
            }
            throw new \Exception($err_msg);
        }

        if ($hashes_cnt !== $have_cnt) {
            $err_msg = "Records count unsynchronized: $hashes_cnt != $have_cnt \n";
            if ($start_repair && method_exists($this, 'restoreHashesFile')) {
                if (!is_file($filename_hashes) || !filesize($filename_hashes)) {
                    $this->restoreHashesFile();
                } elseif (!$have_cnt) {
                    $this->restoreHashesDb();
                } else {
                    $err_msg .= "for start auto-repair: remove damaged file\n";
                    throw new \Exception($err_msg);
                }
            } else {
                throw new \Exception($err_msg);
            }
        }

        if (method_exists($this, 'initDRS')) {
            $drs_cnt = $this->initDRS();
            if ($drs_cnt != $hashes_cnt) {
                throw new \Exception("DRS Records count unsynchronized");
            }
        }
    }
}
