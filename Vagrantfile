# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/bionic64"
  
  #provisionamento através do shell
  config.vm.provision "shell", inline: "apt-get update"
  config.vm.provision "shell", inline: "apt-get install docker"

  config.vm.provision "shell", inline: <<-SHELL
    apt -y install docker.io
  SHELL



  ####### Provision #######
  config.vm.provision "docker" do |docker|
    docker.run "zeppelin",
      image: "apache/zeppelin:0.9.0",
      args: "-p 8080:8080 " + 
            "-v /vagrant/logs:/logs -v /vagrant/notebook:/notebook " +
            "-e ZEPPELIN_LOG_DIR='/logs' -e ZEPPELIN_NOTEBOOK_DIR='/notebook'"
  end

  #config.vm.provision "shell", 
  #inline: "docker run -p 8080:8080 --rm /
  #         -v /vagrant/logs:/logs -v /vagrant/notebook:/notebook /
  #         -e ZEPPELIN_LOG_DIR='/logs' -e ZEPPELIN_NOTEBOOK_DIR='/notebook' /
  #         --name zeppelin apache/zeppelin:0.9.0"

  # Disable automatic box update checking. If you disable this, then
  # boxes will only be checked for updates when the user runs
  # `vagrant box outdated`. This is not recommended.
  # config.vm.box_check_update = false

  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine. In the example below,
  # accessing "localhost:8080" will access port 80 on the guest machine.
  # NOTE: This will enable public access to the opened port
  config.vm.network "forwarded_port", guest: 80, host: 8080

  # Create a private network, which allows host-only access to the machine
  # using a specific IP.
  # config.vm.network "private_network", ip: "192.168.33.10"
  config.vm.network "private_network", type: "dhcp"

  # Create a public network, which generally matched to bridged network.
  # Bridged networks make the machine appear as another physical device on
  # your network.
  # config.vm.network "public_network"

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  config.vm.synced_folder ".", "/vagrant", mount_options: ["dmode=775,fmode=777"]

  config.vm.provider "virtualbox" do |vb|
    # Customize the amount of memory on the VM:
    vb.memory = "4096"
    vb.cpus = 2
    vb.name = "Teste_Engenheiro_De_Dados"
  end
	
end
