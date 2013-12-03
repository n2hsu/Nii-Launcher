/*
 * Copyright (C) 2013 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.n2hsu.launcher;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.n2hsu.launcher.LauncherSettings.Favorites;
import com.n2hsu.launcher.LauncherSettings.WorkspaceScreens;
import com.n2hsu.launcher.backup.BackupProtos;
import com.n2hsu.launcher.backup.BackupProtos.CheckedMessage;
import com.n2hsu.launcher.backup.BackupProtos.Favorite;
import com.n2hsu.launcher.backup.BackupProtos.Journal;
import com.n2hsu.launcher.backup.BackupProtos.Key;
import com.n2hsu.launcher.backup.BackupProtos.Resource;
import com.n2hsu.launcher.backup.BackupProtos.Screen;
import com.n2hsu.launcher.backup.BackupProtos.Widget;

import android.app.backup.BackupDataInputStream;
import android.app.backup.BackupHelper;
import android.app.backup.BackupDataInput;
import android.app.backup.BackupDataOutput;
import android.app.backup.BackupManager;
import android.appwidget.AppWidgetManager;
import android.appwidget.AppWidgetProviderInfo;
import android.content.ComponentName;
import android.content.ContentResolver;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.drawable.Drawable;
import android.os.ParcelFileDescriptor;
import android.text.TextUtils;
import android.util.Base64;
import android.util.Log;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Persist the launcher home state across calamities.
 */
public class LauncherBackupHelper implements BackupHelper {

	private static final String TAG = "LauncherBackupHelper";
	private static final boolean DEBUG = false;
	private static final boolean DEBUG_PAYLOAD = false;

	private static final int MAX_JOURNAL_SIZE = 1000000;

	/** icons are large, dribble them out */
	private static final int MAX_ICONS_PER_PASS = 10;

	/** widgets contain previews, which are very large, dribble them out */
	private static final int MAX_WIDGETS_PER_PASS = 5;

	public static final int IMAGE_COMPRESSION_QUALITY = 75;

	public static final String LAUNCHER_PREFIX = "L";

	private static final Bitmap.CompressFormat IMAGE_FORMAT = android.graphics.Bitmap.CompressFormat.PNG;

	private static BackupManager sBackupManager;

	private static final String[] FAVORITE_PROJECTION = { Favorites._ID, // 0
			Favorites.MODIFIED, // 1
			Favorites.INTENT, // 2
			Favorites.APPWIDGET_PROVIDER, // 3
			Favorites.APPWIDGET_ID, // 4
			Favorites.CELLX, // 5
			Favorites.CELLY, // 6
			Favorites.CONTAINER, // 7
			Favorites.ICON, // 8
			Favorites.ICON_PACKAGE, // 9
			Favorites.ICON_RESOURCE, // 10
			Favorites.ICON_TYPE, // 11
			Favorites.ITEM_TYPE, // 12
			Favorites.SCREEN, // 13
			Favorites.SPANX, // 14
			Favorites.SPANY, // 15
			Favorites.TITLE, // 16
	};

	private static final int ID_INDEX = 0;
	private static final int ID_MODIFIED = 1;
	private static final int INTENT_INDEX = 2;
	private static final int APPWIDGET_PROVIDER_INDEX = 3;
	private static final int APPWIDGET_ID_INDEX = 4;
	private static final int CELLX_INDEX = 5;
	private static final int CELLY_INDEX = 6;
	private static final int CONTAINER_INDEX = 7;
	private static final int ICON_INDEX = 8;
	private static final int ICON_PACKAGE_INDEX = 9;
	private static final int ICON_RESOURCE_INDEX = 10;
	private static final int ICON_TYPE_INDEX = 11;
	private static final int ITEM_TYPE_INDEX = 12;
	private static final int SCREEN_INDEX = 13;
	private static final int SPANX_INDEX = 14;
	private static final int SPANY_INDEX = 15;
	private static final int TITLE_INDEX = 16;

	private static final String[] SCREEN_PROJECTION = { WorkspaceScreens._ID, // 0
			WorkspaceScreens.MODIFIED, // 1
			WorkspaceScreens.SCREEN_RANK // 2
	};

	private static final int SCREEN_RANK_INDEX = 2;

	private final Context mContext;

	private HashMap<ComponentName, AppWidgetProviderInfo> mWidgetMap;

	private ArrayList<Key> mKeys;

	public LauncherBackupHelper(Context context) {
		mContext = context;
	}

	private void dataChanged() {
		if (sBackupManager == null) {
			sBackupManager = new BackupManager(mContext);
		}
		sBackupManager.dataChanged();
	}

	/**
	 * Back up launcher data so we can restore the user's state on a new device.
	 * 
	 * <P>
	 * The journal is a timestamp and a list of keys that were saved as of that
	 * time.
	 * 
	 * <P>
	 * Keys may come back in any order, so each key/value is one complete row of
	 * the database.
	 * 
	 * @param oldState
	 *            notes from the last backup
	 * @param data
	 *            incremental key/value pairs to persist off-device
	 * @param newState
	 *            notes for the next backup
	 * @throws IOException
	 */
	@Override
	public void performBackup(ParcelFileDescriptor oldState, BackupDataOutput data,
			ParcelFileDescriptor newState) {
		Log.v(TAG, "onBackup");

		Journal in = readJournal(oldState);
		Journal out = Journal.newBuilder().setRows(0).setBytes(0l).setT(System.currentTimeMillis()).build();

		long lastBackupTime = in.getT();

		Log.v(TAG, "lastBackupTime=" + lastBackupTime);

		ArrayList<Key> keys = new ArrayList<Key>();
		try {
			backupFavorites(in, data, out, keys);
			backupScreens(in, data, out, keys);
			backupIcons(in, data, out, keys);
			backupWidgets(in, data, out, keys);
		} catch (IOException e) {
			Log.e(TAG, "launcher backup has failed", e);
		}

		// out.key = keys.toArray(BackupProtos.Key.EMPTY_ARRAY);
		writeJournal(newState, Journal.newBuilder(out).addAllKey(null).build());
		Log.v(TAG, "onBackup: wrote " + out.getBytes() + "b in " + out.getRows() + " rows.");
	}

	/**
	 * Restore launcher configuration from the restored data stream.
	 * 
	 * <P>
	 * Keys may arrive in any order.
	 * 
	 * @param data
	 *            the key/value pair from the server
	 */
	@Override
	public void restoreEntity(BackupDataInputStream data) {
		Log.v(TAG, "restoreEntity");
		if (mKeys == null) {
			mKeys = new ArrayList<Key>();
		}
		byte[] buffer = new byte[512];
		String backupKey = data.getKey();
		int dataSize = data.size();
		if (buffer.length < dataSize) {
			buffer = new byte[dataSize];
		}
		Key key = null;
		int bytesRead = 0;
		try {
			bytesRead = data.read(buffer, 0, dataSize);
			if (DEBUG)
				Log.d(TAG, "read " + bytesRead + " of " + dataSize + " available");
		} catch (IOException e) {
			Log.d(TAG, "failed to read entity from restore data", e);
		}
		try {
			key = backupKeyToKey(backupKey);
			switch (key.getType().getNumber()) {
			case Key.Type.FAVORITE_VALUE:
				restoreFavorite(key, buffer, dataSize, mKeys);
				break;

			case Key.Type.SCREEN_VALUE:
				restoreScreen(key, buffer, dataSize, mKeys);
				break;

			case Key.Type.ICON_VALUE:
				restoreIcon(key, buffer, dataSize, mKeys);
				break;

			case Key.Type.WIDGET_VALUE:
				restoreWidget(key, buffer, dataSize, mKeys);
				break;

			default:
				Log.w(TAG, "unknown restore entity type: " + key.getType());
				break;
			}
		} catch (KeyParsingException e) {
			Log.w(TAG, "ignoring unparsable backup key: " + backupKey);
		}

	}

	/**
	 * Record the restore state for the next backup.
	 * 
	 * @param newState
	 *            notes about the backup state after restore.
	 */
	@Override
	public void writeNewStateDescription(ParcelFileDescriptor newState) {
		// clear the output journal time, to force a full backup to
		// will catch any changes the restore process might have made
		Journal out = Journal.newBuilder().setT(0).addAllKey(null).build();

		writeJournal(newState, out);
		Log.v(TAG, "onRestore: read " + mKeys.size() + " rows");
		mKeys.clear();
	}

	/**
	 * Write all modified favorites to the data stream.
	 * 
	 * 
	 * @param in
	 *            notes from last backup
	 * @param data
	 *            output stream for key/value pairs
	 * @param out
	 *            notes about this backup
	 * @param keys
	 *            keys to mark as clean in the notes for next backup
	 * @throws IOException
	 */
	private void backupFavorites(Journal in, BackupDataOutput data, Journal out, ArrayList<Key> keys)
			throws IOException {
		// read the old ID set
		Set<String> savedIds = getSavedIdsByType(Key.Type.FAVORITE_VALUE, in);
		if (DEBUG)
			Log.d(TAG, "favorite savedIds.size()=" + savedIds.size());

		// persist things that have changed since the last backup
		ContentResolver cr = mContext.getContentResolver();
		Cursor cursor = cr.query(Favorites.CONTENT_URI, FAVORITE_PROJECTION, null, null, null);
		Set<String> currentIds = new HashSet<String>(cursor.getCount());
		try {
			cursor.moveToPosition(-1);
			while (cursor.moveToNext()) {
				final long id = cursor.getLong(ID_INDEX);
				final long updateTime = cursor.getLong(ID_MODIFIED);
				Key key = getKey(Key.Type.FAVORITE_VALUE, id);
				keys.add(key);
				currentIds.add(keyToBackupKey(key));
				if (updateTime > in.getT()) {
					byte[] blob = packFavorite(cursor);
					writeRowToBackup(key, blob, out, data);
				}
			}
		} finally {
			cursor.close();
		}
		if (DEBUG)
			Log.d(TAG, "favorite currentIds.size()=" + currentIds.size());

		// these IDs must have been deleted
		savedIds.removeAll(currentIds);
		out = out.newBuilder(out).setRows(out.getRows() + removeDeletedKeysFromBackup(savedIds, data))
				.build();
	}

	/**
	 * Read a favorite from the stream.
	 * 
	 * <P>
	 * Keys arrive in any order, so screens and containers may not exist yet.
	 * 
	 * @param key
	 *            identifier for the row
	 * @param buffer
	 *            the serialized proto from the stream, may be larger than
	 *            dataSize
	 * @param dataSize
	 *            the size of the proto from the stream
	 * @param keys
	 *            keys to mark as clean in the notes for next backup
	 */
	private void restoreFavorite(Key key, byte[] buffer, int dataSize, ArrayList<Key> keys) {
		Log.v(TAG, "unpacking favorite " + key.getId() + " (" + dataSize + " bytes)");
		if (DEBUG)
			Log.d(TAG,
					"read (" + buffer.length + "): "
							+ Base64.encodeToString(buffer, 0, dataSize, Base64.NO_WRAP));

		try {
			Favorite favorite = unpackFavorite(buffer, 0, dataSize);
			if (DEBUG)
				Log.d(TAG, "unpacked " + favorite.getItemType());
		} catch (InvalidProtocolBufferException e) {
			Log.w(TAG, "failed to decode proto", e);
		}
	}

	/**
	 * Write all modified screens to the data stream.
	 * 
	 * 
	 * @param in
	 *            notes from last backup
	 * @param data
	 *            output stream for key/value pairs
	 * @param out
	 *            notes about this backup
	 * @param keys
	 *            keys to mark as clean in the notes for next backup
	 * @throws IOException
	 */
	private void backupScreens(Journal in, BackupDataOutput data, Journal out, ArrayList<Key> keys)
			throws IOException {
		// read the old ID set
		Set<String> savedIds = getSavedIdsByType(Key.Type.SCREEN_VALUE, in);
		if (DEBUG)
			Log.d(TAG, "screen savedIds.size()=" + savedIds.size());

		// persist things that have changed since the last backup
		ContentResolver cr = mContext.getContentResolver();
		Cursor cursor = cr.query(WorkspaceScreens.CONTENT_URI, SCREEN_PROJECTION, null, null, null);
		Set<String> currentIds = new HashSet<String>(cursor.getCount());
		try {
			cursor.moveToPosition(-1);
			while (cursor.moveToNext()) {
				final long id = cursor.getLong(ID_INDEX);
				final long updateTime = cursor.getLong(ID_MODIFIED);
				Key key = getKey(Key.Type.SCREEN_VALUE, id);
				keys.add(key);
				currentIds.add(keyToBackupKey(key));
				if (updateTime > in.getT()) {
					byte[] blob = packScreen(cursor);
					writeRowToBackup(key, blob, out, data);
				}
			}
		} finally {
			cursor.close();
		}
		if (DEBUG)
			Log.d(TAG, "screen currentIds.size()=" + currentIds.size());

		// these IDs must have been deleted
		savedIds.removeAll(currentIds);
		out = out.newBuilder(out).setRows(out.getRows() + removeDeletedKeysFromBackup(savedIds, data))
				.build();
	}

	/**
	 * Read a screen from the stream.
	 * 
	 * <P>
	 * Keys arrive in any order, so children of this screen may already exist.
	 * 
	 * @param key
	 *            identifier for the row
	 * @param buffer
	 *            the serialized proto from the stream, may be larger than
	 *            dataSize
	 * @param dataSize
	 *            the size of the proto from the stream
	 * @param keys
	 *            keys to mark as clean in the notes for next backup
	 */
	private void restoreScreen(Key key, byte[] buffer, int dataSize, ArrayList<Key> keys) {
		Log.v(TAG, "unpacking screen " + key.getId());
		if (DEBUG)
			Log.d(TAG,
					"read (" + buffer.length + "): "
							+ Base64.encodeToString(buffer, 0, dataSize, Base64.NO_WRAP));
		try {
			Screen screen = unpackScreen(buffer, 0, dataSize);
			if (DEBUG)
				Log.d(TAG, "unpacked " + screen.getRank());
		} catch (InvalidProtocolBufferException e) {
			Log.w(TAG, "failed to decode proto", e);
		}
	}

	/**
	 * Write all the static icon resources we need to render placeholders for a
	 * package that is not installed.
	 * 
	 * @param in
	 *            notes from last backup
	 * @param data
	 *            output stream for key/value pairs
	 * @param out
	 *            notes about this backup
	 * @param keys
	 *            keys to mark as clean in the notes for next backup
	 * @throws IOException
	 */
	private void backupIcons(Journal in, BackupDataOutput data, Journal out, ArrayList<Key> keys)
			throws IOException {
		// persist icons that haven't been persisted yet
		final LauncherAppState appState = LauncherAppState.getInstanceNoCreate();
		if (appState == null) {
			dataChanged(); // try again later
			if (DEBUG)
				Log.d(TAG, "Launcher is not initialized, delaying icon backup");
			return;
		}
		final ContentResolver cr = mContext.getContentResolver();
		final IconCache iconCache = appState.getIconCache();
		final int dpi = mContext.getResources().getDisplayMetrics().densityDpi;

		// read the old ID set
		Set<String> savedIds = getSavedIdsByType(Key.Type.ICON_VALUE, in);
		if (DEBUG)
			Log.d(TAG, "icon savedIds.size()=" + savedIds.size());

		int startRows = out.getRows();
		if (DEBUG)
			Log.d(TAG, "starting here: " + startRows);
		String where = Favorites.ITEM_TYPE + "=" + Favorites.ITEM_TYPE_APPLICATION;
		Cursor cursor = cr.query(Favorites.CONTENT_URI, FAVORITE_PROJECTION, where, null, null);
		Set<String> currentIds = new HashSet<String>(cursor.getCount());
		try {
			cursor.moveToPosition(-1);
			while (cursor.moveToNext()) {
				final long id = cursor.getLong(ID_INDEX);
				final String intentDescription = cursor.getString(INTENT_INDEX);
				try {
					Intent intent = Intent.parseUri(intentDescription, 0);
					ComponentName cn = intent.getComponent();
					Key key = null;
					String backupKey = null;
					if (cn != null) {
						key = getKey(Key.Type.ICON_VALUE, cn.flattenToShortString());
						backupKey = keyToBackupKey(key);
						currentIds.add(backupKey);
					} else {
						Log.w(TAG, "empty intent on application favorite: " + id);
					}
					if (savedIds.contains(backupKey)) {
						if (DEBUG)
							Log.d(TAG, "already saved icon " + backupKey);

						// remember that we already backed this up previously
						keys.add(key);
					} else if (backupKey != null) {
						if (DEBUG)
							Log.d(TAG, "I can count this high: " + out.getRows());
						if ((out.getRows() - startRows) < MAX_ICONS_PER_PASS) {
							if (DEBUG)
								Log.d(TAG, "saving icon " + backupKey);
							Bitmap icon = iconCache.getIcon(intent);
							keys.add(key);
							if (icon != null && !iconCache.isDefaultIcon(icon)) {
								byte[] blob = packIcon(dpi, icon);
								writeRowToBackup(key, blob, out, data);
							}
						} else {
							if (DEBUG)
								Log.d(TAG, "scheduling another run for icon " + backupKey);
							// too many icons for this pass, request another.
							dataChanged();
						}
					}
				} catch (URISyntaxException e) {
					Log.w(TAG, "invalid URI on application favorite: " + id);
				} catch (IOException e) {
					Log.w(TAG, "unable to save application icon for favorite: " + id);
				}

			}
		} finally {
			cursor.close();
		}
		if (DEBUG)
			Log.d(TAG, "icon currentIds.size()=" + currentIds.size());

		// these IDs must have been deleted
		savedIds.removeAll(currentIds);
		out = out.newBuilder(out).setRows(out.getRows() + removeDeletedKeysFromBackup(savedIds, data))
				.build();
	}

	/**
	 * Read an icon from the stream.
	 * 
	 * <P>
	 * Keys arrive in any order, so shortcuts that use this icon may already
	 * exist.
	 * 
	 * @param key
	 *            identifier for the row
	 * @param buffer
	 *            the serialized proto from the stream, may be larger than
	 *            dataSize
	 * @param dataSize
	 *            the size of the proto from the stream
	 * @param keys
	 *            keys to mark as clean in the notes for next backup
	 */
	private void restoreIcon(Key key, byte[] buffer, int dataSize, ArrayList<Key> keys) {
		Log.v(TAG, "unpacking icon " + key.getId());
		if (DEBUG)
			Log.d(TAG,
					"read (" + buffer.length + "): "
							+ Base64.encodeToString(buffer, 0, dataSize, Base64.NO_WRAP));
		try {
			Resource res = unpackIcon(buffer, 0, dataSize);
			if (DEBUG)
				Log.d(TAG, "unpacked " + res.getDpi());
			if (DEBUG)
				Log.d(TAG,
						"read "
								+ Base64.encodeToString(res.getData().toByteArray(), 0, res.getData().size(),
										Base64.NO_WRAP));
			Bitmap icon = BitmapFactory.decodeByteArray(res.getData().toByteArray(), 0, res.getData().size());
			if (icon == null) {
				Log.w(TAG, "failed to unpack icon for " + key.getName());
			}
		} catch (InvalidProtocolBufferException e) {
			Log.w(TAG, "failed to decode proto", e);
		}
	}

	/**
	 * Write all the static widget resources we need to render placeholders for
	 * a package that is not installed.
	 * 
	 * @param in
	 *            notes from last backup
	 * @param data
	 *            output stream for key/value pairs
	 * @param out
	 *            notes about this backup
	 * @param keys
	 *            keys to mark as clean in the notes for next backup
	 * @throws IOException
	 */
	private void backupWidgets(Journal in, BackupDataOutput data, Journal out, ArrayList<Key> keys)
			throws IOException {
		// persist static widget info that hasn't been persisted yet
		final LauncherAppState appState = LauncherAppState.getInstanceNoCreate();
		if (appState == null) {
			dataChanged(); // try again later
			if (DEBUG)
				Log.d(TAG, "Launcher is not initialized, delaying widget backup");
			return;
		}
		final ContentResolver cr = mContext.getContentResolver();
		final WidgetPreviewLoader previewLoader = new WidgetPreviewLoader(mContext);
		final PagedViewCellLayout widgetSpacingLayout = new PagedViewCellLayout(mContext);
		final IconCache iconCache = appState.getIconCache();
		final int dpi = mContext.getResources().getDisplayMetrics().densityDpi;
		final DeviceProfile profile = appState.getDynamicGrid().getDeviceProfile();
		if (DEBUG)
			Log.d(TAG, "cellWidthPx: " + profile.cellWidthPx);

		// read the old ID set
		Set<String> savedIds = getSavedIdsByType(Key.Type.WIDGET_VALUE, in);
		if (DEBUG)
			Log.d(TAG, "widgets savedIds.size()=" + savedIds.size());

		int startRows = out.getRows();
		if (DEBUG)
			Log.d(TAG, "starting here: " + startRows);
		String where = Favorites.ITEM_TYPE + "=" + Favorites.ITEM_TYPE_APPWIDGET;
		Cursor cursor = cr.query(Favorites.CONTENT_URI, FAVORITE_PROJECTION, where, null, null);
		Set<String> currentIds = new HashSet<String>(cursor.getCount());
		try {
			cursor.moveToPosition(-1);
			while (cursor.moveToNext()) {
				final long id = cursor.getLong(ID_INDEX);
				final String providerName = cursor.getString(APPWIDGET_PROVIDER_INDEX);
				final int spanX = cursor.getInt(SPANX_INDEX);
				final int spanY = cursor.getInt(SPANY_INDEX);
				final ComponentName provider = ComponentName.unflattenFromString(providerName);
				Key key = null;
				String backupKey = null;
				if (provider != null) {
					key = getKey(Key.Type.WIDGET_VALUE, providerName);
					backupKey = keyToBackupKey(key);
					currentIds.add(backupKey);
				} else {
					Log.w(TAG, "empty intent on appwidget: " + id);
				}
				if (savedIds.contains(backupKey)) {
					if (DEBUG)
						Log.d(TAG, "already saved widget " + backupKey);

					// remember that we already backed this up previously
					keys.add(key);
				} else if (backupKey != null) {
					if (DEBUG)
						Log.d(TAG, "I can count this high: " + out.getRows());
					if ((out.getRows() - startRows) < MAX_WIDGETS_PER_PASS) {
						if (DEBUG)
							Log.d(TAG, "saving widget " + backupKey);
						previewLoader.setPreviewSize(spanX * profile.cellWidthPx, spanY
								* profile.cellHeightPx, widgetSpacingLayout);
						byte[] blob = packWidget(dpi, previewLoader, iconCache, provider);
						keys.add(key);
						writeRowToBackup(key, blob, out, data);

					} else {
						if (DEBUG)
							Log.d(TAG, "scheduling another run for widget " + backupKey);
						// too many widgets for this pass, request another.
						dataChanged();
					}
				}
			}
		} finally {
			cursor.close();
		}
		if (DEBUG)
			Log.d(TAG, "widget currentIds.size()=" + currentIds.size());

		// these IDs must have been deleted
		savedIds.removeAll(currentIds);
		out = out.newBuilder(out).setRows(out.getRows() + removeDeletedKeysFromBackup(savedIds, data))
				.build();
	}

	/**
	 * Read a widget from the stream.
	 * 
	 * <P>
	 * Keys arrive in any order, so widgets that use this data may already
	 * exist.
	 * 
	 * @param key
	 *            identifier for the row
	 * @param buffer
	 *            the serialized proto from the stream, may be larger than
	 *            dataSize
	 * @param dataSize
	 *            the size of the proto from the stream
	 * @param keys
	 *            keys to mark as clean in the notes for next backup
	 */
	private void restoreWidget(Key key, byte[] buffer, int dataSize, ArrayList<Key> keys) {
		Log.v(TAG, "unpacking widget " + key.getId());
		if (DEBUG)
			Log.d(TAG,
					"read (" + buffer.length + "): "
							+ Base64.encodeToString(buffer, 0, dataSize, Base64.NO_WRAP));
		try {
			Widget widget = unpackWidget(buffer, 0, dataSize);
			if (DEBUG)
				Log.d(TAG, "unpacked " + widget.getProvider());
			if (widget.getIcon().getData() != null) {
				Bitmap icon = BitmapFactory.decodeByteArray(widget.getIcon().getData().toByteArray(), 0,
						widget.getIcon().getData().size());
				if (icon == null) {
					Log.w(TAG, "failed to unpack widget icon for " + key.getName());
				}
			}
		} catch (InvalidProtocolBufferException e) {
			Log.w(TAG, "failed to decode proto", e);
		}
	}

	/**
	 * create a new key, with an integer ID.
	 * 
	 * <P>
	 * Keys contain their own checksum instead of using the heavy-weight
	 * CheckedMessage wrapper.
	 */
	private Key getKey(int type, long id) {
		Key key = Key.newBuilder().setId(id).setType(Key.Type.valueOf(type)).build();

		return Key.newBuilder(key).setChecksum(checkKey(key)).build();
	}

	/**
	 * create a new key for a named object.
	 * 
	 * <P>
	 * Keys contain their own checksum instead of using the heavy-weight
	 * CheckedMessage wrapper.
	 */
	private Key getKey(int type, String name) {

		Key key = Key.newBuilder().setName(name).setType(Key.Type.valueOf(type)).build();

		return Key.newBuilder(key).setChecksum(checkKey(key)).build();
	}

	/** keys need to be strings, serialize and encode. */
	private String keyToBackupKey(Key key) {
		return Base64.encodeToString(key.toByteArray(), Base64.NO_WRAP);
	}

	/** keys need to be strings, decode and parse. */
	private Key backupKeyToKey(String backupKey) throws KeyParsingException {
		try {
			Key key = Key.parseFrom(Base64.decode(backupKey, Base64.DEFAULT));
			if (key.getChecksum() != checkKey(key)) {
				key = null;
				throw new KeyParsingException("invalid key read from stream" + backupKey);
			}
			return key;
		} catch (InvalidProtocolBufferException e) {
			throw new KeyParsingException(e);
		} catch (IllegalArgumentException e) {
			throw new KeyParsingException(e);
		}
	}

	private String getKeyName(Key key) {
		if (TextUtils.isEmpty(key.getName())) {
			return Long.toString(key.getId());
		} else {
			return key.getName();
		}

	}

	private String geKeyType(Key key) {
		switch (key.getType().getNumber()) {
		case Key.Type.FAVORITE_VALUE:
			return "favorite";
		case Key.Type.SCREEN_VALUE:
			return "screen";
		case Key.Type.ICON_VALUE:
			return "icon";
		case Key.Type.WIDGET_VALUE:
			return "widget";
		default:
			return "anonymous";
		}
	}

	/** Compute the checksum over the important bits of a key. */
	private long checkKey(Key key) {
		CRC32 checksum = new CRC32();
		checksum.update(key.getType().getNumber());
		checksum.update((int) (key.getId() & 0xffff));
		checksum.update((int) ((key.getId() >> 32) & 0xffff));
		if (!TextUtils.isEmpty(key.getName())) {
			checksum.update(key.getName().getBytes());
		}
		return checksum.getValue();
	}

	/** Serialize a Favorite for persistence, including a checksum wrapper. */
	private byte[] packFavorite(Cursor c) {
		Favorite favorite = Favorite.newBuilder().setId(c.getLong(ID_INDEX))
				.setScreen(c.getInt(SCREEN_INDEX)).setSpanX(c.getInt(SPANX_INDEX))
				.setSpanY(c.getInt(SPANY_INDEX)).setContainer(c.getInt(CONTAINER_INDEX))
				.setCellX(c.getInt(CELLX_INDEX)).setCellY(c.getInt(CELLY_INDEX))
				.setIconType(c.getInt(ICON_TYPE_INDEX)).build();

		if (favorite.getIconType() == Favorites.ICON_TYPE_RESOURCE) {
			String iconPackage = c.getString(ICON_PACKAGE_INDEX);
			if (!TextUtils.isEmpty(iconPackage)) {
				favorite = Favorite.newBuilder(favorite).setIconPackage(iconPackage).build();
			}
			String iconResource = c.getString(ICON_RESOURCE_INDEX);
			if (!TextUtils.isEmpty(iconResource)) {
				favorite = Favorite.newBuilder(favorite).setIconResource(iconResource).build();
			}
		}
		if (favorite.getIconType() == Favorites.ICON_TYPE_BITMAP) {
			byte[] blob = c.getBlob(ICON_INDEX);
			if (blob != null && blob.length > 0) {

				favorite = Favorite.newBuilder(favorite).setIcon(ByteString.copyFrom(blob)).build();
			}
		}
		String title = c.getString(TITLE_INDEX);
		if (!TextUtils.isEmpty(title)) {
			favorite = Favorite.newBuilder(favorite).setTitle(title).build();
		}
		String intent = c.getString(INTENT_INDEX);
		if (!TextUtils.isEmpty(intent)) {
			favorite = Favorite.newBuilder(favorite).setIntent(intent).build();
		}

		favorite = Favorite.newBuilder(favorite).setItemType(c.getInt(ITEM_TYPE_INDEX)).build();

		if (favorite.getItemType() == Favorites.ITEM_TYPE_APPWIDGET) {

			favorite = Favorite.newBuilder(favorite).setAppWidgetId(c.getInt(APPWIDGET_ID_INDEX)).build();

			String appWidgetProvider = c.getString(APPWIDGET_PROVIDER_INDEX);
			if (!TextUtils.isEmpty(appWidgetProvider)) {

				favorite = Favorite.newBuilder(favorite).setAppWidgetProvider(appWidgetProvider).build();
			}
		}

		return writeCheckedBytes(favorite);
	}

	/**
	 * Deserialize a Favorite from persistence, after verifying checksum
	 * wrapper.
	 */
	private Favorite unpackFavorite(byte[] buffer, int offset, int dataSize)
			throws InvalidProtocolBufferException {
		Favorite favorite = Favorite.parseFrom(readCheckedBytes(buffer, offset, dataSize));
		return favorite;
	}

	/** Serialize a Screen for persistence, including a checksum wrapper. */
	private byte[] packScreen(Cursor c) {
		Screen screen = Screen.newBuilder().setId(c.getLong(ID_INDEX)).setRank(c.getInt(SCREEN_RANK_INDEX))
				.build();

		return writeCheckedBytes(screen);
	}

	/** Deserialize a Screen from persistence, after verifying checksum wrapper. */
	private Screen unpackScreen(byte[] buffer, int offset, int dataSize)
			throws InvalidProtocolBufferException {
		Screen screen = Screen.parseFrom(readCheckedBytes(buffer, offset, dataSize));
		return screen;
	}

	/**
	 * Serialize an icon Resource for persistence, including a checksum wrapper.
	 */
	private byte[] packIcon(int dpi, Bitmap icon) {
		Resource res = Resource.newBuilder().setDpi(dpi).build();

		ByteArrayOutputStream os = new ByteArrayOutputStream();
		if (icon.compress(IMAGE_FORMAT, IMAGE_COMPRESSION_QUALITY, os)) {
			res = Resource.newBuilder(res).setData(ByteString.copyFrom(os.toByteArray())).build();
		}
		return writeCheckedBytes(res);
	}

	/**
	 * Deserialize an icon resource from persistence, after verifying checksum
	 * wrapper.
	 */
	private Resource unpackIcon(byte[] buffer, int offset, int dataSize)
			throws InvalidProtocolBufferException {
		Resource res = Resource.parseFrom(readCheckedBytes(buffer, offset, dataSize));
		return res;
	}

	/** Serialize a widget for persistence, including a checksum wrapper. */
	private byte[] packWidget(int dpi, WidgetPreviewLoader previewLoader, IconCache iconCache,
			ComponentName provider) {
		final AppWidgetProviderInfo info = findAppWidgetProviderInfo(provider);

		Widget widget = Widget.newBuilder().setProvider(provider.flattenToShortString()).setLabel(info.label)
				.setConfigure(info.configure != null).build();
		if (info.icon != 0) {

			Resource res = Resource.newBuilder().build();
			widget = Widget.newBuilder(widget).setIcon(res).build();

			Drawable fullResIcon = iconCache.getFullResIcon(provider.getPackageName(), info.icon);
			Bitmap icon = Utilities.createIconBitmap(fullResIcon, mContext);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			if (icon.compress(IMAGE_FORMAT, IMAGE_COMPRESSION_QUALITY, os)) {

				res = Resource.newBuilder(res).setData(ByteString.copyFrom(os.toByteArray())).setDpi(dpi)
						.build();
				widget = Widget.newBuilder(widget).setIcon(res).build();
			}
		}
		if (info.previewImage != 0) {

			Resource preview = Resource.newBuilder().build();
			widget = Widget.newBuilder(widget).setPreview(preview).build();

			Bitmap previewBitmap = previewLoader.generateWidgetPreview(info, null);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			if (previewBitmap.compress(IMAGE_FORMAT, IMAGE_COMPRESSION_QUALITY, os)) {

				preview = Resource.newBuilder(preview).setData(ByteString.copyFrom(os.toByteArray()))
						.setDpi(dpi).build();
				widget = Widget.newBuilder(widget).setPreview(preview).build();
			}
		}
		return writeCheckedBytes(widget);
	}

	/** Deserialize a widget from persistence, after verifying checksum wrapper. */
	private Widget unpackWidget(byte[] buffer, int offset, int dataSize)
			throws InvalidProtocolBufferException {
		Widget widget = Widget.parseFrom(readCheckedBytes(buffer, offset, dataSize));
		return widget;
	}

	/**
	 * Read the old journal from the input file.
	 * 
	 * In the event of any error, just pretend we didn't have a journal, in that
	 * case, do a full backup.
	 * 
	 * @param oldState
	 *            the read-0only file descriptor pointing to the old journal
	 * @return a Journal protocol bugffer
	 */
	private Journal readJournal(ParcelFileDescriptor oldState) {
		Journal journal = Journal.newBuilder().build();
		if (oldState == null) {
			return journal;
		}
		FileInputStream inStream = new FileInputStream(oldState.getFileDescriptor());
		try {
			int remaining = inStream.available();
			if (DEBUG)
				Log.d(TAG, "available " + remaining);
			if (remaining < MAX_JOURNAL_SIZE) {
				byte[] buffer = new byte[remaining];
				int bytesRead = 0;
				while (remaining > 0) {
					try {
						int result = inStream.read(buffer, bytesRead, remaining);
						if (result > 0) {
							if (DEBUG)
								Log.d(TAG, "read some bytes: " + result);
							remaining -= result;
							bytesRead += result;
						} else {
							// stop reading ands see what there is to parse
							Log.w(TAG, "read error: " + result);
							remaining = 0;
						}
					} catch (IOException e) {
						Log.w(TAG, "failed to read the journal", e);
						buffer = null;
						remaining = 0;
					}
				}
				if (DEBUG)
					Log.d(TAG, "journal bytes read: " + bytesRead);

				if (buffer != null) {
					try {
						journal = journal.parseFrom(readCheckedBytes(buffer, 0, bytesRead));
					} catch (InvalidProtocolBufferException e) {
						Log.d(TAG, "failed to read the journal", e);
						journal = null;
					}
				}
			}
		} catch (IOException e) {
			Log.d(TAG, "failed to close the journal", e);
		} finally {
			try {
				inStream.close();
			} catch (IOException e) {
				Log.d(TAG, "failed to close the journal", e);
			}
		}
		return journal;
	}

	private void writeRowToBackup(Key key, byte[] blob, Journal out, BackupDataOutput data)
			throws IOException {
		String backupKey = keyToBackupKey(key);
		data.writeEntityHeader(backupKey, blob.length);
		data.writeEntityData(blob, blob.length);

		out = Journal.newBuilder(out).setRows(out.getRows() + 1).setBytes(out.getBytes() + blob.length)
				.build();

		Log.v(TAG, "saving " + geKeyType(key) + " " + backupKey + ": " + getKeyName(key) + "/" + blob.length);
		if (DEBUG_PAYLOAD) {
			String encoded = Base64.encodeToString(blob, 0, blob.length, Base64.NO_WRAP);
			final int chunkSize = 1024;
			for (int offset = 0; offset < encoded.length(); offset += chunkSize) {
				int end = offset + chunkSize;
				end = Math.min(end, encoded.length());
				Log.d(TAG, "wrote " + encoded.substring(offset, end));
			}
		}
	}

	private Set<String> getSavedIdsByType(int type, Journal in) {
		Set<String> savedIds = new HashSet<String>();
		for (int i = 0; i < in.getKeyList().size(); i++) {
			Key key = in.getKey(i);
			if (key.getType().ordinal() == type) {
				savedIds.add(keyToBackupKey(key));
			}
		}
		return savedIds;
	}

	private int removeDeletedKeysFromBackup(Set<String> deletedIds, BackupDataOutput data) throws IOException {
		int rows = 0;
		for (String deleted : deletedIds) {
			Log.v(TAG, "dropping icon " + deleted);
			data.writeEntityHeader(deleted, -1);
			rows++;
		}
		return rows;
	}

	/**
	 * Write the new journal to the output file.
	 * 
	 * In the event of any error, just pretend we didn't have a journal, in that
	 * case, do a full backup.
	 * 
	 * @param newState
	 *            the write-only file descriptor pointing to the new journal
	 * @param journal
	 *            a Journal protocol buffer
	 */
	private void writeJournal(ParcelFileDescriptor newState, Journal journal) {
		FileOutputStream outStream = null;
		try {
			outStream = new FileOutputStream(newState.getFileDescriptor());
			outStream.write(writeCheckedBytes(journal));
			outStream.close();
		} catch (IOException e) {
			Log.d(TAG, "failed to write backup journal", e);
		}
	}

	/** Wrap a proto in a CheckedMessage and compute the checksum. */
	private byte[] writeCheckedBytes(Message proto) {

		CheckedMessage wrapper = CheckedMessage.newBuilder().build();

		wrapper = CheckedMessage.newBuilder(wrapper).setPayload(proto.toByteString()).build();

		CRC32 checksum = new CRC32();
		checksum.update(wrapper.getPayload().size());

		wrapper = CheckedMessage.newBuilder(wrapper).setChecksum(checksum.getValue()).build();

		return wrapper.toByteArray();
	}

	/** Unwrap a proto message from a CheckedMessage, verifying the checksum. */
	private byte[] readCheckedBytes(byte[] buffer, int offset, int dataSize)
			throws InvalidProtocolBufferException {

		byte[] newByte = new byte[dataSize];
		for (int i = offset; i < (offset + dataSize); i++) {
			newByte[i - offset] = buffer[i];
		}
		CheckedMessage wrapper = CheckedMessage.newBuilder().setPayload(ByteString.copyFrom(newByte)).build();

		CRC32 checksum = new CRC32();
		checksum.update(wrapper.getPayload().size());
		if (wrapper.getChecksum() != checksum.getValue()) {
			throw new InvalidProtocolBufferException("checksum does not match");
		}
		return wrapper.getPayload().toByteArray();
	}

	private AppWidgetProviderInfo findAppWidgetProviderInfo(ComponentName component) {
		if (mWidgetMap == null) {
			List<AppWidgetProviderInfo> widgets = AppWidgetManager.getInstance(mContext)
					.getInstalledProviders();
			mWidgetMap = new HashMap<ComponentName, AppWidgetProviderInfo>(widgets.size());
			for (AppWidgetProviderInfo info : widgets) {
				mWidgetMap.put(info.provider, info);
			}
		}
		return mWidgetMap.get(component);
	}

	private class KeyParsingException extends Throwable {
		private KeyParsingException(Throwable cause) {
			super(cause);
		}

		public KeyParsingException(String reason) {
			super(reason);
		}
	}
}
