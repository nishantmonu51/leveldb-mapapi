package nl.gridline.leveldb;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import nl.gridline.leveldb.bindings.StringBinding;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test {@link LevelDBStoredMap} clear, and putAll batch operations. All else is tested through {@link MapTest}
 * @author <a href="mailto:job@gridline.nl">Job</a>
 */
public class LevelDBStoredMapTest
{

	private File empty;
	private File populated;

	@Before
	public void before() throws IOException
	{
		empty = Files.createTempDirectory("batchop-test").toFile();
		populated = Files.createTempDirectory("batchop-test").toFile();
	}

	@After
	public void after()
	{
		FileUtils.deleteRecursively(empty);
		FileUtils.deleteRecursively(populated);
	}

	@Test
	public void testClearBelowLimit0()
	{
		Map<String, String> map = makePopulatedMap(0);
		assertTrue(map.isEmpty());
		map.clear();
		assertTrue(map.isEmpty());
	}

	@Test
	public void testClearBelowLimit2()
	{
		Map<String, String> map = makePopulatedMap(2);
		map.clear();
		assertTrue(map.isEmpty());
	}

	@Test
	public void testClearBelowLimit999()
	{
		Map<String, String> map = makePopulatedMap(999);
		map.clear();
		assertTrue(map.isEmpty());
	}

	@Test
	public void testClearAtLimit1000()
	{
		Map<String, String> map = makePopulatedMap(1000);
		map.clear();
		assertTrue(map.isEmpty());
	}

	@Test
	public void testClearAboveLimit1500()
	{
		Map<String, String> map = makePopulatedMap(1500);
		map.clear();
		assertTrue(map.isEmpty());
	}

	@Test
	public void testClearAboveLimit2050()
	{
		Map<String, String> map = makePopulatedMap(2050);
		map.clear();
		assertTrue(map.isEmpty());
	}

	private Map<String, String> makePopulatedMap(int size)
	{
		FileUtils.deleteRecursively(populated);

		Options options = new Options();
		options.createIfMissing(true);
		EntryBinding<String> stringBinding = new StringBinding();
		DB db = null;
		try
		{
			db = factory.open(populated, options);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		LevelDBStoredMap<String, String> result = new LevelDBStoredMap<String, String>(db, stringBinding, stringBinding);
		for (int i = 0; i < size; i++)
		{
			result.put("key-" + i, "value-" + i);
		}
		return result;
	}

	protected Map<String, String> makeEmptyMap()
	{
		FileUtils.deleteRecursively(empty);

		Options options = new Options();
		options.createIfMissing(true);
		EntryBinding<String> stringBinding = new StringBinding();
		DB db = null;
		try
		{
			db = factory.open(empty, options);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		return new LevelDBStoredMap<String, String>(db, stringBinding, stringBinding);
	}

	private Map<String, String> makePopulatedHashmap(int size)
	{
		Map<String, String> result = new HashMap<>();
		for (int i = 0; i < size; i++)
		{
			result.put("key-" + i, "value-" + i);
		}
		return result;
	}

	@Test
	public void testPutAllBelowLimit0() throws Exception
	{
		Map<String, String> map = makePopulatedHashmap(2);
		Map<String, String> stored = makeEmptyMap();
		stored.putAll(map);
		assertEquals(map.size(), stored.size());
	}

	@Test
	public void testPutAllBelowLimit2() throws Exception
	{
		Map<String, String> map = makePopulatedHashmap(2);
		Map<String, String> stored = makeEmptyMap();
		stored.putAll(map);
		assertEquals(map.size(), stored.size());
	}

	@Test
	public void testPutAllBelowLimit999() throws Exception
	{
		Map<String, String> map = makePopulatedHashmap(999);
		Map<String, String> stored = makeEmptyMap();
		stored.putAll(map);
		assertEquals(map.size(), stored.size());
	}

	@Test
	public void testPutAllAtLimit1000() throws Exception
	{
		Map<String, String> map = makePopulatedHashmap(1000);
		Map<String, String> stored = makeEmptyMap();
		stored.putAll(map);
		assertEquals(map.size(), stored.size());
	}

	@Test
	public void testPutAllAboveLimit1500() throws Exception
	{
		Map<String, String> map = makePopulatedHashmap(1500);
		Map<String, String> stored = makeEmptyMap();
		stored.putAll(map);
		assertEquals(map.size(), stored.size());
	}

	@Test
	public void testPutAllAboveLimit2050() throws Exception
	{
		Map<String, String> map = makePopulatedHashmap(1500);
		Map<String, String> stored = makeEmptyMap();
		stored.putAll(map);
		assertEquals(map.size(), stored.size());
	}

}
