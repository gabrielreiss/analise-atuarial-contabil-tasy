call activate produtos

echo "Exportando os dados dos indices contabeis"
python -W ignore "C:\Users\gabriel.castro\Desktop\Augusto\Guia dos Dashboards\Indices-Contabeis\src\python\exportar_indices_certo.py"

echo "Exportando PIC"
python -W ignore "C:\Users\gabriel.castro\Desktop\Augusto\Guia dos Dashboards\Indices-Contabeis\src\python\exportar_pic.py"

echo "Exportando inadimplencia"
python -W ignore "C:\Users\gabriel.castro\Desktop\Augusto\Guia dos Dashboards\Indices-Contabeis\src\python\inadimplencia.py"
