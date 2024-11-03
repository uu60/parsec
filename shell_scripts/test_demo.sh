#!/bin/zsh

hosts=("ub1" "ub2" "ub3")
cd "$(dirname $(readlink -f "$0"))"
cd ../
dir_to_upload="$PWD"
cd ../
demo_dir="$PWD/demo"

for host in "${hosts[@]}"; do
    ssh "$host" "rm -rf ~/$(basename "$dir_to_upload") ~/demo"

    if [ "$host" = "${hosts[0]}" ]; then
      rsync -av --exclude '*_1.h' "$dir_to_upload" "$host:~/"
      rsync -av --exclude 'commands.txt' "$demo_dir" "$host:~/"
    elif [ "$host" = "${hosts[1]}" ]; then
      rsync -av --exclude '*_0.h' "$dir_to_upload" "$host:~/"
      rsync -av --exclude 'commands.txt' "$demo_dir" "$host:~/"
    else
      rsync -av "$dir_to_upload" "$host:~/"
      rsync -av "$demo_dir" "$host:~/"
    fi

    ssh "$host" "export PATH=/usr/local/openmpi/bin:\$PATH && export LD_LIBRARY_PATH=/usr/local/openmpi/lib:\$LD_LIBRARY_PATH && cd ~/demo && sh ../mpc-package/shell_scripts/install.sh && cmake . && make"
done
echo "DONE"
echo "Please run 'mpirun -np 3 -hostfile ../hostfile.txt demo -case 5' on a client."