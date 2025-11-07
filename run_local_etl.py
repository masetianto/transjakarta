
import pandas as pd
from pathlib import Path

root = Path(__file__).parents[1] / 'data'

bus = pd.read_csv(root / 'input' / 'dummy_transaksi_bus.csv')
halte = pd.read_csv(root / 'input' / 'dummy_transaksi_halte.csv')

df = pd.concat([bus, halte], ignore_index=True)
df.drop_duplicates(inplace=True)
df['no_body_var'] = df['no_body_var'].astype(str).str.upper().str.replace('_','-').str.replace(' ','-')
def fmt_nb(x):
    parts = x.split('-')
    prefix = parts[0]
    suffix = ''.join(filter(str.isdigit, parts[-1]))
    if suffix == '':
        suffix = '0'
    return f"{prefix}-{int(suffix):03d}"
df['no_body_var'] = df['no_body_var'].apply(fmt_nb)

df_pelanggan = df[df['status_var']=='S']

by_card = df_pelanggan.groupby(['tanggal','card_type','gate_in_boo']).agg({'amount':'sum','pelanggan_id':'count'}).reset_index()
by_card.rename(columns={'pelanggan_id':'jumlah_pelanggan','amount':'total_amount'}, inplace=True)
by_card.to_csv(root / 'output' / 'pelanggan_by_cardtype.csv', index=False)

by_route = df_pelanggan.groupby(['tanggal','route_code','route_name','gate_in_boo']).agg({'amount':'sum','pelanggan_id':'count'}).reset_index()
by_route.rename(columns={'pelanggan_id':'jumlah_pelanggan','amount':'total_amount'}, inplace=True)
by_route.to_csv(root / 'output' / 'pelanggan_by_route.csv', index=False)

by_tarif = df_pelanggan.groupby(['tanggal','tarif','gate_in_boo']).agg({'amount':'sum','pelanggan_id':'count'}).reset_index()
by_tarif.rename(columns={'pelanggan_id':'jumlah_pelanggan','amount':'total_amount'}, inplace=True)
by_tarif.to_csv(root / 'output' / 'pelanggan_by_tarif.csv', index=False)

print('Local ETL finished. Output files are in data/output/')
