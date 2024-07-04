import psycopg2
import data

def main():

    conn = psycopg2.connect("postgresql://tvdi_h021_user:6SkYneWBM90wc7Eq9U5VfXwliSQBrOno@dpg-cpscse08fa8c739534a0-a.singapore-postgres.render.com/tvdi_h021")
    with conn: #with conn會自動commit(),手動close
        with conn.cursor() as cursor: #自動close()
            sql = '''
            CREATE TABLE IF NOT EXISTS youbike(
            _id Serial Primary Key,
            sna VARCHAR(50) NOT NULL,
            sarea VARCHAR(50),
            ar VARCHAR(100),
            mday timestamp,
            updateTime timestamp,
            total smallint,
            rent_bikes smallint,
            return_bikes smallint,
            lat REAL,
            lng REAL,
            act boolean,
            UNIQUE(sna, updateTime) 
            );
            '''
            cursor.execute(sql)
        
        all_data:list[dict] = data.load_data()

    with conn.cursor() as cursor:            
            insert_sql = '''
            INSERT INTO youbike(sna, sarea, ar, mday, updateTime, total, rent_bikes, return_bikes, lat, lng, act)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sna, updateTime) DO NOTHING;
            '''
            for site in all_data:
                cursor.execute(insert_sql,(site['sna'],
                                site['sarea'],
                                site['ar'],
                                site['mday'],
                                site['updateTime'],
                                site['total'],
                                site['rent_bikes'],
                                site['retuen_bikes'],
                                site['lat'],
                                site['lng'],
                                site['act']
                                ))
    conn.close()



if __name__ =='__main__':
    main()