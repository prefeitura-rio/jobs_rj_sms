# -*- coding: utf-8 -*-

def format_bytes(size):
	# [Ref] https://stackoverflow.com/a/49361727/4824627
	power = 2**10  # 2**10 = 1024
	n = 0
	power_labels = {0 : "", 1: "K", 2: "M", 3: "G", 4: "T", 5: "P"}
	while size > power:
		size /= power
		n += 1
	return f"{size:.2f} {power_labels[n]}B"
