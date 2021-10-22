# Trade情報収集App

Trading botに使用できそうな特徴量を収集するアプリケーション。

pybottersライブラリを用いることにより複数取引所に対応した非同期I/Oである実装となっている。

pybotters: https://github.com/MtkN1/pybotters

下記の情報を5秒間隔で取得し、日付が変わるたびにGCS (Google Cloud Storage) にデータを保存する。

* ohlcv情報
* 板情報
* OI（未決済建玉）
* Liquidations（精算）

24時間稼働させるため、クラウド（AWSやGCPなど）のVM上での運用を推奨。

## Installation

Trade情報収集Appを動作させる分には下記のrequirements.txtで十分である。

```sh
pip install requirements.txt
```

notebook内の機械学習ライブラリを使用するために、conda環境を用いたinstallを下記のシェルスクリプトに記載している。

```sh
./pip_install.sh
```

なお、python version 3.9系で動作確認済みである。

## Usage

1. 収集したTrade情報をGCS (Google Cloud Storage) に保存しているため、GCPのアカウントが必要である。

   下記の記事を参考にGCP環境をセットアップすること。

   https://qiita.com/Brutus/items/22dfd31a681b67837a74

2. GCSの設定を行い、サービスアカウントキー: jsonファイルを作成。

   作成したjsonファイルを `secret_key.json` にリネームして、`main_ticker.py` と同じ階層に配置する。

   参考: https://qiita.com/komiya_____/items/e933bd9e0dcd9079cfbb

3. スクリプトファイル `run.sh` を実行することで、バックグラウンドでプログラムが動作する。

   ```sh
   ./run.sh
   ```

## Note

* 現在はbybitの取引所にのみ対応している。TODO: 複数の取引所に対応

* 非同期I/Oの大元のコードは `trading_api/trading_api.py` に記載している

  