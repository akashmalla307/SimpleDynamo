package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.Cursor;
import android.nfc.Tag;
import android.util.Log;

import java.util.HashMap;

public class DatabaseHandler extends SQLiteOpenHelper  {

    static final String TAG = DatabaseHandler.class.getSimpleName();
    private static final int DATABASE_VERSION = 4;
    private static final String DATABASE_NAME = "SimpleDynamo";
    private static final String TABLE_KEYPAIR = "KeyValue";
    private static final String KEY1 = "key1";
    private static final String VALUE = "value";
    private static final String AVD1 = "avd1";
    private static final String AVD2 = "avd2";
    private static final String AVD3 = "avd3";
    private static final String VERSION = "version";

    public DatabaseHandler(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);

    }


    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        String CREATE_TABLE =
                "CREATE TABLE " + TABLE_KEYPAIR + "("
                        + KEY1 + " TEXT ,"
                        + VALUE + " TEXT,"
                        + AVD1 + "  TEXT, "
                        + AVD2 + "  TEXT, "
                        + AVD3 + "  TEXT, "
                        + VERSION + "  TEXT "
                        + ")";
        Log.v(TAG,"create table cmd = "+CREATE_TABLE);
        sqLiteDatabase.execSQL(CREATE_TABLE);
    }
    void addKeyPair(ContentValues cv) {
        SQLiteDatabase db = this.getWritableDatabase();
        String countQuery = "SELECT  * FROM " + TABLE_KEYPAIR + " where "+ KEY1+"='"+cv.get("key") +"'" ;
        Cursor  cursor = db.rawQuery(countQuery,null);
        int count = cursor.getCount();
       // if(count>0){
       //     updatekeypair(cv);
       // }else {
            ContentValues values = new ContentValues();
            values.put(KEY1, String.valueOf(cv.get("key"))); // Contact Name
            values.put(VALUE, String.valueOf(cv.get("value"))); // Contact Name
            values.put(AVD1, String.valueOf(cv.get("avd1"))); // Contact Name
            values.put(AVD2, String.valueOf(cv.get("avd2"))); // Contact Name
            values.put(AVD3, String.valueOf(cv.get("avd3"))); // Contact Name


        String sql =
                "INSERT INTO KeyValue (key1, value, avd1, avd2,avd3,version) VALUES('"+cv.get("key")+"','"+cv.get("value")+"','"+cv.get("avd1")+"','"+cv.get("avd2")+"','"+cv.get("avd3")+"','"+cv.get("ver")+"' )" ;
        Log.v(TAG,"before insert for key = "+ cv.get("key")+" "+ sql);
        db.execSQL(sql);

        String sql1 = "Select * from KeyValue";
        Cursor  cursor1 = db.rawQuery(sql1,null);
        if (!(cursor1.moveToFirst()) || cursor1.getCount() ==0){
            cursor1.moveToFirst();
        }
        String key2 =  cursor1.getString(cursor1.getColumnIndex("key1"));
        Log.v(TAG,"In DB cursor count= "+cursor1.getCount() +" key = "+key2);

        // }
        //2nd argument is String containing nullColumnHack
       // db.close(); // Closing database connection
    }


    void addKeyPair2(String sqlstr) {
        SQLiteDatabase db = this.getWritableDatabase();

        String sql1 = "Select * from KeyValue";
        Cursor  cursor1 = db.rawQuery(sql1,null);
        if (!(cursor1.moveToFirst()) || cursor1.getCount() ==0){
            cursor1.moveToFirst();
        }
        //String key2 =  cursor1.getString(cursor1.getColumnIndex("key1"));
        Log.v(TAG,"by  comeback  keys In DB before cursor count= "+cursor1.getCount());


        String sql =
                "INSERT INTO KeyValue (key1, value, avd1, avd2,avd3,version) VALUES "+sqlstr;
        db.execSQL(sql);

        String sql2 = "Select * from KeyValue";
        Cursor  cursor2 = db.rawQuery(sql2,null);
        if (!(cursor2.moveToFirst()) || cursor2.getCount() ==0){
            cursor2.moveToFirst();
        }
        //String key2 =  cursor1.getString(cursor1.getColumnIndex("key1"));
        Log.v(TAG,"in comeback keys In DB after cursor count= "+cursor2.getCount());

    }

    // code to get the single contact
   public Cursor getValue(String key1) {
        SQLiteDatabase db = this.getReadableDatabase();

        String query ="Select key1,value,max(version) as ver from KeyValue where key1 = '"+key1+"'  group by '"+key1+"' having max(version)";
        Cursor cr = db.rawQuery(query,null);
        /*Cursor cursor = db.query(TABLE_KEYPAIR, new String[] { VALUE }, KEY1+ "=?",
                new String[] { String.valueOf(cv.get("key")) }, null, null, null, null);*/
        String valuename = null;
        if(cr.getCount()>0){
            Log.v(TAG,"found here");
        }else{
            return null;
        }
        if (cr.moveToFirst()) {
           // while (cr.isAfterLast() != true) {
                 valuename =  cr.getString(cr.getColumnIndex("value"));
           // }
            Log.v(TAG,"inside getValue Value="+valuename);
        }

        return cr;
    }

    // code to get all contacts in a list view
    public Cursor getComebackKeys(ContentValues cv) {
        SQLiteDatabase db = this.getWritableDatabase();
        Log.v(TAG,"Inside getComebackKeys = "+cv);
        // Select All Query
        String selectQuery = "SELECT  key1,value,avd1,avd2,avd3,version FROM KeyValue where avd1='" + cv.get("avd") + "' OR avd2= '" + cv.get("avd") + "' OR avd3= '" + cv.get("avd") + "'";

        Cursor cursor = db.rawQuery(selectQuery, null);
        try {
            if (cursor != null) {
                cursor.moveToFirst();
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }

        return cursor;
    }

    public int getCount() {
        SQLiteDatabase db = this.getWritableDatabase();
        Log.v(TAG,"inside getCount");
        String sql1 = "Select * from KeyValue group by key1";
        Cursor  cursor1 = db.rawQuery(sql1,null);
        if (!(cursor1.moveToFirst()) || cursor1.getCount() ==0){
            cursor1.moveToFirst();
        }
        Log.v(TAG,"count of getCount = "+ cursor1.getCount());
        return cursor1.getCount();
    }

    public Cursor getMaxVersion(ContentValues cv) {
        SQLiteDatabase db = this.getWritableDatabase();
        Log.v(TAG,"inside getCount");
        String sql1 = "Select key1,value,max(version) as ver from KeyValue where key1 = '"+cv.get("key")+"' group by  key1 having max(version)";
        Log.v(TAG,"before maxversion for key = "+ cv.get("key")+" "+ sql1);

        Cursor  cursor1 = db.rawQuery(sql1,null);
        //Cursor  cursor1  = db.query("KeyValue",new String[]{"key1","value","version"},null,null,"key1","MAX(version)",null);


        String sql2 = "Select key1,value,max(version) as ver from KeyValue group by  key1 having max(version)";
        Cursor  cursor2 = db.rawQuery(sql1,null);
        Log.v(TAG,"count of cursor2 for key = "+cv.get("key")+" "+ cursor2.getCount());
        if(cursor2!=null){
            cursor2.moveToFirst();
            do{
                String key =cursor2.getString(cursor2.getColumnIndex("key1"));
                String value =cursor2.getString(cursor2.getColumnIndex("value"));
                String ver =cursor2.getString(cursor2.getColumnIndex("ver"));
                Log.v(TAG,"key n value n ver = "+key+" "+value+" "+ver);
            }while(cursor2.moveToNext());
        }

        int ver=0;
        try {
            if (cursor1 != null) {
                try {
                    if (!(cursor1.moveToFirst()) || cursor1.getCount() == 0) {
                        cursor1.moveToFirst();
                        ver = Integer.valueOf(cursor1.getString(cursor1.getColumnIndex("ver")));
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                Log.v(TAG, "max version for key = " + cv.get("key") + " " + cv.get("value") + " " + cv.get("ver"));
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }

        return  cursor1 ;
    }

    public Cursor getAllloaclKeys() {
        SQLiteDatabase db = this.getWritableDatabase();
        // Select All Query
        String selectQuery = "SELECT  key1 , value, max(version) as ver FROM KeyValue group by key1 having max(version)";
        Log.v(TAG,"inside getalllocalkeys "+ selectQuery);
        Cursor cursor = db.rawQuery(selectQuery, null);
        try {
            if (cursor != null) {
                cursor.moveToFirst();
                String valuename = cursor.getString(cursor.getColumnIndex("key1"));
                Log.v(TAG, "Value=" + valuename);
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
        Log.v(TAG,"cursor in @ ="+cursor.getCount());
        return cursor;
    }

    // Deleting single contact
    public void deletekey(ContentValues cv) {
        SQLiteDatabase db = this.getWritableDatabase();
        String delete = "Delete from KeyValue where key1='"+cv.get("key")+"'";
        Cursor cr = db.rawQuery(delete,null);
        Log.v(TAG,"cursor count in delete key = "+cr.getCount());
        db.close();
    }

    public void deletetable() {
        SQLiteDatabase db = this.getWritableDatabase();
        String delete = "Delete from KeyValue ";
        Cursor cr = db.rawQuery(delete,null);
        Log.v(TAG,"cursor count in delete table = "+cr.getCount());
        db.close();
    }



        @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
// Drop older table if existed
            sqLiteDatabase.execSQL("DROP TABLE IF EXISTS " + TABLE_KEYPAIR);

            // Create tables again
            onCreate(sqLiteDatabase);
    }
}
/*

 Cursor[] cursors = new Cursor[2];
        cursors[0] = mDatabase.query(TABLE5_NAME, null, null, null, null, null, null);
        assertEquals(1, cursors[0].getCount());
        createCursors();
        cursors[1] = mCursors[1];
        assertTrue(cursors[1].getCount() > 0);
        MergeCursor mergeCursor = new MergeCursor(cursors);
        // MergeCursor should points to cursors[0] after moveToFirst.
        mergeCursor.moveToFirst();
 */