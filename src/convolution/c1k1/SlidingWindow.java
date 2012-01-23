package convolution.c1k1;

import java.util.LinkedList;

/**
 * SlidingWindow
 * 
 * Very simple sliding window
 * 
 */
public class SlidingWindow {

	
	LinkedList<Long> oCurrentWindow;
	
	long _lWindowSize;
	
	public SlidingWindow( long WindowSize ) {
	
		this._lWindowSize = WindowSize;
		
		this.oCurrentWindow = new LinkedList<Long>();
		
	}
	
	public long GetWindowSize() {
		return this._lWindowSize;
	}
	
	
	public boolean WindowIsFull() {
		
		if ( this.oCurrentWindow.size() >= this._lWindowSize ) {
			return true;
		}
		
		return false;
		
	}
	
	public void AddPoint( long point ) throws Exception {

		this.oCurrentWindow.add( point );
		
	}
	
	/**
	 * Slide the window forward
	 * - burn off the first half of the window
	 * - still must re-add more points from the Reduce iterator
	 * @throws Exception
	 */
	public void SlideWindowForward() {
		
		this.oCurrentWindow.removeFirst();
	}	


	public LinkedList<Long> GetCurrentWindow() {
		
		return this.oCurrentWindow;
		
	}

	
}
