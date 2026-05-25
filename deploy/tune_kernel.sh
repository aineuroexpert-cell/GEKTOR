#!/bin/bash
echo "🛑 [1/3] Injecting GRUB parameters..."
sed -i 's/GRUB_CMDLINE_LINUX_DEFAULT="[^"]*/& isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3 processor.max_cstate=0 intel_idle.max_cstate=0 pcie_aspm=off tsc=reliable/' /etc/default/grub
update-grub

echo "🛑 [2/3] Configuring CPU Governor..."
cpupower frequency-set -g performance

echo "🛑 [3/3] Disabling irqbalance and rerouting interrupts..."
systemctl stop irqbalance
systemctl disable irqbalance
# Все прерывания только на ядра 0 и 1 (битовая маска 3 -> 00000011)
echo 3 > /proc/irq/default_smp_affinity
for irq in $(ls /proc/irq/ | grep -E '^[0-9]+$'); do
    echo 3 > /proc/irq/$irq/smp_affinity 2>/dev/null
done

echo "✅ Reboot required to apply Ring 0 isolation."
