#!/bin/sh
set -o errexit

echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries
sed -i 's@//.*archive.ubuntu.com@//mirrors.ustc.edu.cn@g' /etc/apt/sources.list
sed -i -e 's@//ports.ubuntu.com/\? @//ports.ubuntu.com/ubuntu-ports @g' \
            -e 's@//ports.ubuntu.com@//mirrors.ustc.edu.cn@g' \
            /etc/apt/sources.list

apt-get update
apt-get install -y \
  apt-transport-https \
  gnupg \
  wget

# we need LLVM >= 3.9 for onig_sys/bindgen

cat <<-EOF > /etc/apt/sources.list.d/llvm.list
deb http://mirrors.tuna.tsinghua.edu.cn/llvm-apt/xenial/ llvm-toolchain-xenial-9 main
#deb-src http://mirrors.tuna.tsinghua.edu.cn/llvm-apt/xenial/ llvm-toolchain-xenial-9 main
EOF

wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key| apt-key add -

apt-get update

# needed by onig_sys
apt-get install -y \
      libclang1-9 \
      llvm-9 \
      unzip
