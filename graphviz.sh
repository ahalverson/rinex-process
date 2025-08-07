find . -type d -print0 | \
  gawk -v q='"' -v ORS=' ' 'BEGIN{print "digraph g {"} {gsub(q, "\\"q"); a[NR]=$0; for(i=NR-1; i>0; i--) if(index($0, a[i])==1 && substr($0, length(a[i])+1, 1)=="/") {print q a[i] q " -> " q $0 q; break}} END{print "}"}' RS='\0' | \
  dot -Tsvg -o tree.svg
