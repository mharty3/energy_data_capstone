#!/bin/bash

# Run using the below command
# bash vm_setup.sh
#!/bin/bash

# Run using the below command
# bash vm_setup.sh

echo hello $USER

if [ -f ~/setup_result.txt ]; 
then

cat ~/setup_result.txt

else
sudo apt-add-repository ppa:fish-shell/release-3 -y
sudo apt-get update
sudo apt install wget

echo "installing miniconda..."
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
~/miniconda3/bin/conda init zsh

echo "Installing Docker..."
sudo apt-get -y install docker.io

echo "Docker without sudo setup..."
sudo groupadd docker
sudo gpasswd -a $USER docker
sudo service docker restart

echo "Installing docker-compose..."
cd
mkdir -p bin
cd bin
wget https://github.com/docker/compose/releases/download/v2.3.3/docker-compose-linux-x86_64 -O docker-compose
sudo chmod +x docker-compose

echo "Setup .bashrc..."
echo '' >> ~/.bashrc
echo 'export PATH=${HOME}/bin:${PATH}' >> ~/.bashrc
eval "$(cat ~/.bashrc | tail -n +10)" # A hack because source .bashrc doesn't work inside the script

echo "docker-compose version..."
docker-compose --version

sudo apt -y install fish
echo '' >> ~/.bashrc
echo 'exec fish' >> ~/.bashrc

echo "The setup script vm_init.sh ran successfully on at `date`" >> ~/setup_result.txt

fi

