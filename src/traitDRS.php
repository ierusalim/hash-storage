<?php
namespace HashStorage;
/**
 * $hs->addData(hash-number, data-string)
 * $hs->getData(hash-number) -- return string data
 * $hs->addHashData(hash, data) -- addHash + addData -- return hash-number
 * $hs->getHashData(hash) -- return string data by hash
 */
trait traitDRS
{
    public $drs;

    public function initDRS()
    {
        $drs_path = $this->base_path. $this->trait . '-drs';
        if (!is_dir($drs_path)) {
            mkdir($drs_path);
        }

        $this->drs = new \ierusalim\FileRecords\DRS(
                $drs_path . DIRECTORY_SEPARATOR . $this->trait, [
                'records_per_file' => 100000,
                'bytes_for_offset' => 4,
                'bytes_for_length' => 4
            ]
        );
        return $this->drs->recordsCount();
    }

    public function addData($hash_n, $data)
    {
        $rec_n = (int)$this->drs->appendRecord($data);
        if ($rec_n === $hash_n) {
            return $rec_n;
        }
        throw new \Exception("DRS records not synchronized");
    }

    public function getData($hash_n)
    {
        return $this->drs->readRecord($hash_n);
    }

    public function addHashData($hash_hex_or_bin, $data)
    {
        $hash_n = $this->addHash($hash_hex_or_bin);
        if ($hash_n === false) {
            return false;
        }
        return $this->addData($hash_n, $data);
    }

    public function getHashData($hash_hex_or_bin)
    {
        $hash_n = $this->getHashNum($hash_hex_or_bin);
        if ($hash_n === false) {
            return false;
        }
        return $this->getData($hash_n);
    }
}