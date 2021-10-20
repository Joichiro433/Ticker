# Trade情報収集App

Trading botに使用できそうな特徴量を収集するアプリケーション。

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







